package spouts;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import util.Constants;
import util.EnvironmentVariablesHelper;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Implementation of custom Kafka spout that is used as data source
 *
 * @author Peter Lipcak, Masaryk University
 */
public class CustomKafkaSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Consumer<String, String> consumer;


    /**
     * Opens connection to Kafka broker and starts listening for incoming consumptions
     * @param conf Storm configuration
     * @param context context of Storm application
     * @param collector emits incoming records to the topology
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;

        //Kafka configuration properties
        String kafkaIP = EnvironmentVariablesHelper.getKafkaIpPort();
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaIP);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //Create Kafka consumer and start listening for consumption topic
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(Constants.CONSUMPTIONS_DATA_TOPIC));
    }

    /**
     * This method is initiaded when system is ready for more data to be processed
     */
    @Override
    public void nextTuple() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> record : records){
            _collector.emit(new Values(record.value()));
        }
    }


    /**
     * Declares output fields when emitted to topology
     * @param declarer used to declare output
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value"));
    }

}
