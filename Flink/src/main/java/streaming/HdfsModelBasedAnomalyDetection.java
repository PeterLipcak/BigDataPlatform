package streaming;

import entities.Consumption;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;

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

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import utils.KafkaHelper;

public class HdfsModelBasedAnomalyDetection {

    //    private String kafkaIpPort = "localhost:9092";
//    private String zookeeperIpPort = "localhost:2181";
//    private String uri = "hdfs://localhost:9000/consumptions/expected/1550066951732";
    private static Logger LOG = LoggerFactory.getLogger(HdfsModelBasedAnomalyDetection.class);

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

//        FlinkKafkaProducer<String> metrics = new FlinkKafkaProducer<>(
////                kafkaIpPort,            // broker list
//                "metrics",                  // target topic
//                new SimpleStringSchema(),
//                properties);
//        kafkaStream.filter(record -> {
//            Consumption consumption = new Consumption(record);
//            Consumption firstConsumption = new Consumption("100,2014-10-15 10:45:00,0.0");
//            Consumption lastConsumption = new Consumption("1,2015-12-31 23:59:00,0.715816667");
//            if (consumption.equals(firstConsumption)) {
//                return true;
//            } else if (consumption.equals(lastConsumption)) {
//                return true;
//            }
//
//            return false;
//        }).map(record -> new Date().toString()).addSink(metrics);


        FlinkKafkaProducer<String> anomalies = new FlinkKafkaProducer<>(
//                kafkaIpPort,            // broker list
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


//        final DataStreamSource<Map<String, String>> dataStreamSource =  env.fromElements(expectedConsumptions);


        DataStream<Tuple4<String, Integer, String, Double>> consumptionsWithCompositeId = kafkaStream.map(new MapFunction<String, Tuple4<String, Integer, String, Double>>() {
            @Override
            public Tuple4<String, Integer, String, Double> map(String row) throws ParseException {
                String[] splits = row.split(",");
                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(splits[1]);
                DateTime consumptionDate = new DateTime(date);
                String compositeId = splits[0] + "-" + consumptionDate.getYear() + "-" + consumptionDate.getDayOfYear() + "-" + consumptionDate.getHourOfDay();
                return new Tuple4(compositeId, Integer.parseInt(splits[0]), splits[1], Double.parseDouble(splits[2]));
            }
        });

        consumptionsWithCompositeId.rebalance().filter(consumption -> {
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
