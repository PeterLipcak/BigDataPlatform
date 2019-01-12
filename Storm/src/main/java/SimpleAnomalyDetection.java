import entities.Consumption;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.*;

public class SimpleAnomalyDetection {

    public static class KafkaInputProccessor extends BaseBasicBolt {

        private Integer limit = null;
        private HttpClient client = null;
        private Consumption firstRow = null;
        private Consumption lastRow = null;

        public KafkaInputProccessor(Integer limit){
            this.limit = limit;
        }

        public KafkaInputProccessor(Integer limit, Consumption firstRow, Consumption lastRow){
            this.limit = limit;
            this.firstRow = firstRow;
            this.lastRow = lastRow;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            try {
                client = new HttpClient("http://localhost:9090");
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }

        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String key = tuple.getStringByField("key");
            try {
                Consumption consumption = new Consumption(tuple.getStringByField("value"));

                if(firstRow != null && lastRow != null){
                    if(consumption.equals(firstRow)){
                        collector.emit(new Values(key, "START TIME: " + new Date().toString()));
                    }else if(consumption.equals(lastRow)){
                        collector.emit(new Values(key, "END TIME: " + new Date().toString()));
                    }
                }


                if(consumption.getConsumption() > limit){
                    MetricBuilder builder = MetricBuilder.getInstance();
                    builder.addMetric("anomaly")
                            .addTag("customerId", String.valueOf(consumption.getId()))
                            .addDataPoint(consumption.getMeasurementTimestamp().getTime(), consumption.getConsumption());
                    client.pushMetrics(builder);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (IOException e) {
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
        Integer limit = Integer.parseInt(args[0]);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("127.0.0.1:9092", "test").build()), 1);

        if (args.length > 1){
            Consumption firstRow = new Consumption(args[1]);
            Consumption lastRow = new Consumption(args[2]);
            builder.setBolt("kafka_input_bolt", new KafkaInputProccessor(limit,firstRow,lastRow)).shuffleGrouping("kafka_spout");
        }else{
            builder.setBolt("kafka_input_bolt", new KafkaInputProccessor(limit)).shuffleGrouping("kafka_spout");
        }


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("test-output"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message"));

        builder.setBolt("forwardToKafka", bolt).shuffleGrouping("kafka_input_bolt");

        Config conf = new Config();

        StormSubmitter.submitTopology("kafkaboltTest", conf, builder.createTopology());
    }

}
