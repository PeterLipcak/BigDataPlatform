package streaming;

import bolts.AnomalyDetectionBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.TopologyBuilder;
import spouts.CustomKafkaSpout;
import util.EnvironmentVariablesHelper;
import java.util.*;


/**
 * Implementation of model based anomaly detection stream processing job
 *
 * @author Peter Lipcak, Masaryk University
 */
public class ModelBasedAnomalyDetection {

    private static String kafkaUri = EnvironmentVariablesHelper.getKafkaIpPort();

    public static void main(String[] args) throws Exception {
        //Create topology -- consumptions -> anomalies -> anomaliesToKafka
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("consumptions", new CustomKafkaSpout());
        builder.setBolt("anomalies", new AnomalyDetectionBolt(),6).shuffleGrouping("consumptions");

        //Configurational properties for Kafka bolt anomalies
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUri);
        props.put("acks", "0");
        props.put("batch.size", "65536");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Create Kafka bolt and assign to topology
        KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("anomalies"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message"));
        builder.setBolt("anomaliesToKafka", bolt,1).shuffleGrouping("anomalies");

        //Storm configuration
        Config conf = new Config();
        conf.setNumWorkers(8);
        conf.setNumAckers(1);

        //Submit the topology to Storm
        StormSubmitter.submitTopology("anomalyDetection", conf, builder.createTopology());
    }

}
