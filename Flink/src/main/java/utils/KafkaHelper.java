package utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaHelper {

    public static Properties getDefaultKafkaProperties(){
        String kafkaIP = System.getenv("KAFKA_IP_PORT");
        String kafkaUri = kafkaIP != null ? kafkaIP : "localhost:9092";

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUri);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    public static void sendToKafka(String topic, String message){
        Producer<String, String> producer = new KafkaProducer<>(getDefaultKafkaProperties());
        producer.send(new ProducerRecord<String, String>(topic, message));
        producer.close();
    }

}
