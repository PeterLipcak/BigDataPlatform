import entities.Consumption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HdfsModelBasedAnomalyDetection {

    private static String kafkaUri;
    private static String zookeeperUri;


    public static class KafkaInputProccessor extends BaseBasicBolt {

        public void execute(Tuple tuple, BasicOutputCollector collector) {
            try {
                Consumption consumption = new Consumption(tuple.getStringByField("value"));
                collector.emit(new Values(consumption.getCompositeId(),consumption.getConsumption()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "message"));
        }
    }


    public static class AnomalyDetectionProccessor extends BaseBasicBolt {

        private Map<String, Double> expectedConsumptions;
        private String hdfsUri;

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            try {
                String hdfsIP = System.getenv("HDFS_IP_PORT");
                hdfsUri = hdfsIP != null ? "hdfs://" + hdfsIP : "hdfs://localhost:9000";

                Configuration hdfsConf = new Configuration();
                hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
                hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
                hdfsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
                hdfsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                FileSystem fs = FileSystem.get(new URI(hdfsUri + "/consumptions/expected/1550066951732"), hdfsConf);
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

                this.expectedConsumptions = expectedConsumptions;
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String compositeId = tuple.getStringByField("key");
            Double consumption = tuple.getDoubleByField("message");
            Double expectedConsumption = expectedConsumptions.get(compositeId) + 8;
            if(consumption != null && expectedConsumption<consumption){
                collector.emit(new Values(compositeId,compositeId+ ", consumption=" + consumption + " expected=" + expectedConsumption));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "message"));
        }
    }

    public static void main(String[] args) throws Exception {
        initEnvironmentVariables();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("consumptions", new KafkaSpout<>(KafkaSpoutConfig.builder(kafkaUri, "consumptions").build()));
        builder.setBolt("recordsWithCompositeId", new KafkaInputProccessor()).shuffleGrouping("consumptions");
        builder.setBolt("anomalies", new AnomalyDetectionProccessor()).shuffleGrouping("recordsWithCompositeId");

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUri);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("anomalies"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message"));

        builder.setBolt("anomaliesToKafka", bolt).shuffleGrouping("anomalies");

        Config conf = new Config();
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);
//        conf.setNumWorkers(4);

        StormSubmitter.submitTopology("anomalyDetection", conf, builder.createTopology());
    }

    public static void initEnvironmentVariables(){
        String kafkaIP = System.getenv("KAFKA_IP_PORT");
        String zookeeperIP = System.getenv("ZOOKEEPER_IP_PORT");

        kafkaUri = kafkaIP != null ? kafkaIP : "localhost:9092";
        zookeeperUri = zookeeperIP != null ? zookeeperIP : "localhost:2181";
    }

}
