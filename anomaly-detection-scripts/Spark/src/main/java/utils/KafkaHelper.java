package utils;

import entities.Consumption;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static utils.Constants.TIMER_TOPIC;


public class KafkaHelper implements Serializable {

    public static Map<String, Object> getDefaultKafkaParams(){
        String kafkaIpPort = System.getenv("KAFKA_IP_PORT");
        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "localhost:9092");
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
