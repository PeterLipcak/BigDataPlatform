package streaming;

import entities.Consumption;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.KafkaHelper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ModelBasedAnomalyDetection2 {

    //    private String kafkaIpPort = "localhost:9092";
//    private String zookeeperIpPort = "localhost:2181";
//    private String uri = "hdfs://localhost:9000/consumptions/expected/1550066951732";
    private static Logger LOG = LoggerFactory.getLogger(streaming.HdfsModelBasedAnomalyDetection.class);

    private static String kafkaUri;
    private static String zookeeperUri;
    private static String hdfsUri;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        initEnvironmentVariables();

        LOG.info("VARIABLES: HDFS:" + hdfsUri + " KAFKA:" + kafkaUri + " ZOOKEPER:" + zookeeperUri);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaUri);
        properties.setProperty("zookeeper.connect", zookeeperUri);
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumptions = new FlinkKafkaConsumer<>("consumptions", new SimpleStringSchema(), properties);

        DataStream<String> kafkaStream = env
                .addSource(consumptions);

        FlinkKafkaProducer<String> anomalies = new FlinkKafkaProducer<>(
                "anomalies",                  // target topic
                new SimpleStringSchema(),
                properties);

        Configuration hdfsConf = new Configuration();
        hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        hdfsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hdfsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(hdfsUri + "/consumptions/expected/1550066951732"), hdfsConf);
        } catch (Exception e) {
            LOG.info(e.toString());
        }
        FileStatus[] fileStatus = fs.listStatus(new Path(hdfsUri + "/consumptions/expected/1550066951732"));

        final Map<String, Double> expectedConsumptions = new HashMap<>();
        for (FileStatus status : fileStatus) {
            InputStream is = fs.open(status.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                try {
                    String[] splits = line.split(",");
                    expectedConsumptions.put(splits[0], Double.parseDouble(splits[1]));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
        fs.close();

        final OutputTag<String> outputTag = new OutputTag<String>("metrics-output"){};

        SingleOutputStreamOperator<Tuple4<String, Integer, String, Double>> mainDataStream = kafkaStream
                .process(new ProcessFunction<String, Tuple4<String, Integer, String, Double>>() {

                    @Override
                    public void processElement(
                            String row,
                            Context ctx,
                            Collector<Tuple4<String, Integer, String, Double>> out) throws Exception {

                        Consumption consumption = new Consumption(row);
                        Consumption firstConsumption = new Consumption("100,2014-10-15 10:45:00,0.0");
                        Consumption lastConsumption = new Consumption("1,2015-12-31 23:59:00,0.715816667");
                        if (consumption.equals(firstConsumption)) {
                            ctx.output(outputTag, "FIRST " + new Date().toString());
                        } else if (consumption.equals(lastConsumption)) {
                            LOG.info("LAST " + new Date().toString());
                            KafkaHelper.sendToKafka("metrics", "LAST " + new Date().toString());
                            ctx.output(outputTag, "LAST " + new Date().toString());
                        }

                        String[] splits = row.split(",");
                        Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(splits[1]);
                        DateTime consumptionDate = new DateTime(date);
                        String compositeId = splits[0] + "-" + consumptionDate.getYear() + "-" + consumptionDate.getDayOfYear() + "-" + consumptionDate.getHourOfDay();

                        out.collect(new Tuple4(compositeId, Integer.parseInt(splits[0]), splits[1], Double.parseDouble(splits[2])));

                    }
                });

        FlinkKafkaProducer<String> metrics = new FlinkKafkaProducer<>(
//                kafkaIpPort,            // broker list
                "metrics",                  // target topic
                new SimpleStringSchema(),
                properties);

        mainDataStream.getSideOutput(outputTag).addSink(metrics);

        mainDataStream.rebalance().filter(consumption -> {
            Double expectedConsumption = expectedConsumptions.get(consumption.f0);
            return expectedConsumption != null && expectedConsumption.doubleValue() + 7 < consumption.f3;
        }).map(c -> c.toString()).rebalance().addSink(anomalies);

        env.execute();
    }

    public static void initEnvironmentVariables() {
        String kafkaIP = System.getenv("KAFKA_IP_PORT");
        String zookeeperIP = System.getenv("ZOOKEEPER_IP_PORT");
        String hdfsIP = System.getenv("HDFS_IP_PORT");

        kafkaUri = kafkaIP != null ? kafkaIP : "localhost:9092";
        zookeeperUri = zookeeperIP != null ? zookeeperIP : "localhost:2181";
        hdfsUri = hdfsIP != null ? "hdfs://" + hdfsIP : "hdfs://localhost:9000";
    }

}



