import entities.Consumption;
import org.apache.hadoop.fs.FileStatus;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.hdfs.spout.TextFileReader;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.Map;
import java.util.Properties;

public class ReadHdfs {


    public static class KafkaInputProccessor extends BaseBasicBolt {

        private Integer limit;

        public KafkaInputProccessor(Integer limit){
            this.limit = limit;
        }

        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String key = tuple.getStringByField("key");
            try {
                Consumption consumption = new Consumption(tuple.getStringByField("value"));

                if(consumption.getConsumption() > limit){
                    collector.emit(new Values(key,consumption.toString()));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "message"));
        }
    }

    public static void main(String[] args) throws Exception {

        if(args.length == 0){
            System.out.println("argument is required: limit");
        }

//        Configuration hdfsConf = new Configuration();
//        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000/"), hdfsConf);
//        FileStatus[] fileStatus = fs.listStatus(new Path("hdfs://localhost:9000/"));
//        for(FileStatus status : fileStatus){
//            System.out.println(status.getPath().toString());
//
//        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("consumptions", new KafkaSpout<>(KafkaSpoutConfig.builder("127.0.0.1:9092", "consumptions").build()), 1);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("anomalies"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message"));

        builder.setBolt("anomaliesToKafka", bolt).shuffleGrouping("anomalies");

        Config conf = new Config();

        StormSubmitter.submitTopology("anomalyDetection", conf, builder.createTopology());
    }

}
