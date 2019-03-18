import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.hdfs.spout.TextFileReader;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Properties;

public class HdfsToKafka {

    public static class HdfsInputProccessor extends BaseBasicBolt {

        public void execute(Tuple tuple, BasicOutputCollector collector) {
//            String key = tuple.getStringByField("key");

            if(tuple == null || tuple.toString().isEmpty())return;
            collector.emit(new Values(tuple.toString(),tuple.toString()));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "message"));
        }
    }

    public static void main(String[] args) throws Exception {

        HdfsSpout expectedConsumptionsSpout = new HdfsSpout().setReaderType("text")
                .withOutputFields(TextFileReader.defaultFields)
                .setHdfsUri("hdfs://localhost:9000")  // reqd
                .setSourceDir("/consumptions/expected/1550066951732")              // reqd
                .setArchiveDir("/consumptions/expected/1550066951732")           // reqd
                .setBadFilesDir("/data/badfiles");     // required

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("expectedConsumptions", expectedConsumptionsSpout, 1);
        builder.setBolt("expectedConsumptionsProcessed", new HdfsInputProccessor()).shuffleGrouping("expectedConsumptions");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt<String, String> boltExpected = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("expectedConsumptions"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message"));

        builder.setBolt("expectedToKafka", boltExpected).shuffleGrouping("expectedConsumptionsProcessed");

        Config conf = new Config();
        StormSubmitter.submitTopology("anomalyDetection", conf, builder.createTopology());
    }


}
