import densification.IDensificator;
import densification.InterpolationDensificator;
import densification.MultiplierDensificator;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import utils.NumberFormatter;

public class Main {

    private static String kafkaUri;
    private static String zookeeperUri;
    private static String hdfsUri;
    private static String topic;
    private static int limitRecordsPerSecond;
    private static Path path;
    private static Map<String,String> lastRecords = new HashMap<>();
    private static int recordsSent = 0;
    private static int allRecordsSent = 0;
    private static int densificationCount = 1;
    private static int densificationType;
    private static DateTime lastTimestamp;

    private static String timerRecord = null;

    private static String[] interpolators = null;
    private static int idPosition = -1;

    private static final int DENSIFICATION_NONE = 0;
    private static final int DENSIFICATION_MULTIPLICATION = 1;
    private static final int DENSIFICATION_INTERPOLATION = 2;

    public static void main(String... args) {
        Options options = new Options();
        options.addRequiredOption("t", "topic", true, "choose kafka topic");
        options.addRequiredOption("p", "path", true, "hdfs path to dataset");
        options.addOption("id", "id-position", true, "position of id if present");
        options.addRequiredOption("rps", "records-per-second", true, "records per second sent to kafka");
        options.addRequiredOption("dt", "densification-type", true, "densification type [0=None,1=Multiplication,2=Interpolation]");
        options.addOption("dc", "densification-count", true, "how many times to densify dataset [2-1000]");
        options.addOption("hdfs", "hdfs-uri", true, "hdfs uri [default: hdfs://localhost:9000 or HDFS_IP_PORT env variable]");
        options.addOption("kafka", "kafka-uri", true, "kafka uri [default: localhost:9092 or KAFKA_IP_PORT env variable]");
        options.addOption("zk", "zookeeper-uri", true, "zookeeper uri [default: localhost:2181 or ZOOKEEPER_IP_PORT env variable]");
        options.addOption("tr", "timer-record", true, "record which is going to be sent to timer topic with timestamp");

        Option interpolatorsOption = new Option("i", "interpolator", true, "interpolator positions and types [syntax: position|type{|dateFroamt}] {available types: Double, Integer, Long, Date}");
        interpolatorsOption.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(interpolatorsOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("ingestion manager", options);
            System.exit(1);
        }

        topic = cmd.getOptionValue("topic");
        path = new Path(cmd.getOptionValue("path"));
        limitRecordsPerSecond = Integer.parseInt(cmd.getOptionValue("records-per-second"));
        densificationType = Integer.parseInt(cmd.getOptionValue("densification-type"));
        if(densificationType != DENSIFICATION_NONE) densificationCount = Integer.parseInt(cmd.getOptionValue("densification-count"));
        if(densificationType == DENSIFICATION_INTERPOLATION){
            interpolators = cmd.getOptionValues("interpolator");
            if(interpolators.length < 1){
                System.out.println("At least one interpolator required!");
                System.exit(1);
            }
            if(cmd.hasOption("id-position")){
                idPosition = Integer.parseInt(cmd.getOptionValue("id-position"));
            }
        }

        if(cmd.hasOption("timer-record")){
            timerRecord = cmd.getOptionValue("timer-record");
        }

        initEnvironmentVariables();

        if(cmd.hasOption("hdfs-uri"))hdfsUri = cmd.getOptionValue("hdfs-uri");
        if(cmd.hasOption("kafka-uri"))kafkaUri = cmd.getOptionValue("kafka-uri");
        if(cmd.hasOption("zk-uri"))zookeeperUri = cmd.getOptionValue("zk-uri");

        try {
            runIngestion();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Ingestion failed!");
            System.exit(1);
        }

    }

    public static void runIngestion() throws IOException, InterruptedException, java.text.ParseException {
        System.out.println(hdfsUri);
        Configuration hdfsConf = new Configuration();
        hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(hdfsConf);


        DateTime before = new DateTime();
        lastTimestamp = new DateTime();
        if(fs.isFile(path)){
            readAndSendFileToKafka(fs,path);
        }else{
            FileStatus[] fileStatus = fs.listStatus(new Path(hdfsUri+path));
            for(FileStatus status : fileStatus){
                readAndSendFileToKafka(fs, status.getPath());
            }
        }
        DateTime after = new DateTime();
        Duration duration = new Duration(before,after);
        allRecordsSent += recordsSent;

        System.out.println(duration.getStandardHours() + "h " + duration.getStandardMinutes() + "m " + duration.getStandardSeconds() + "s");
        System.out.println(allRecordsSent + " records sent");

        fs.close();
    }

    private static void readAndSendFileToKafka(FileSystem fs, Path path) throws IOException, InterruptedException, java.text.ParseException {
        InputStream is = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Producer<String, String> producer = new KafkaProducer<>(getKafkaProps());
        IDensificator densificator;

        switch (densificationType){
            case DENSIFICATION_NONE : {
                densificator = new MultiplierDensificator(1);
                break;
            }
            case DENSIFICATION_MULTIPLICATION : {
                densificator = new MultiplierDensificator(densificationCount);
                break;
            }
            case DENSIFICATION_INTERPOLATION : {
                densificator = new InterpolationDensificator(densificationCount,interpolators);
                break;
            }
            default: densificator = new MultiplierDensificator(1);
        }


        String line;
        while ((line = br.readLine()) != null) {
            regulateSpeed();
            if(timerRecord != null && timerRecord.equals(line))
                producer.send(new ProducerRecord<>("timer",Long.toString(System.currentTimeMillis()), "GENERATED: " + new DateTime().toString()));

            String[] recordSplits = line.split(",");
            String id = idPosition >= 0 ? recordSplits[idPosition] : "generic-id";
            String lastRecord = lastRecords.get(id);
            lastRecords.put(id,line);

            List<String> recordsToIngest = densificator.densify(lastRecord,line);

            for(String record : recordsToIngest){
                producer.send(new ProducerRecord<>(topic,Long.toString(System.currentTimeMillis()), record));
                recordsSent++;
            }
        }

        producer.close();
    }

    private static void regulateSpeed() throws InterruptedException {
        if(recordsSent>limitRecordsPerSecond){
            DateTime newTimestamp = new DateTime();
            Duration duration = new Duration(lastTimestamp,newTimestamp);

            if(duration.getStandardSeconds()<1){
                Thread.sleep((1000L-duration.getMillis()) * 2);
            }

            lastTimestamp = newTimestamp;
            allRecordsSent += recordsSent;
            recordsSent = 0;

            System.out.println(NumberFormatter.format(allRecordsSent));
        }
    }

    public static void initEnvironmentVariables(){
        String kafkaIP = System.getenv("KAFKA_IP_PORT");
        String zookeeperIP = System.getenv("ZOOKEEPER_IP_PORT");
        String hdfsIP = System.getenv("HDFS_IP_PORT");

        kafkaUri = kafkaIP != null ? kafkaIP : "localhost:9092";
        zookeeperUri = zookeeperIP != null ? zookeeperIP : "localhost:2181";
        hdfsUri = hdfsIP != null ? "hdfs://" + hdfsIP : "hdfs://localhost:9000";
    }

    private static Properties getKafkaProps(){
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUri);
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("request.timeout.ms", 60000);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

}
