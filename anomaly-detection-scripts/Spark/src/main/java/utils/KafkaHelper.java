package utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper methods for Kafka
 * @author Peter Lipcak, Masaryk University
 */
public class KafkaHelper implements Serializable {

    /**
     * Default Kafka configurations with ip and port of Kafka brokers
     * @return default Kafka configuration
     */
    public static Map<String, Object> getDefaultKafkaParams(){
        String kafkaIpPort = EnvironmentVariablesHelper.getKafkaIpPort();
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaIpPort);
        kafkaParams.put("group.id", "test");
        kafkaParams.put("enable.auto.commit", "true");
        kafkaParams.put("auto.commit.interval.ms", "10000");
        kafkaParams.put("session.timeout.ms", "10000");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return kafkaParams;
    }

}