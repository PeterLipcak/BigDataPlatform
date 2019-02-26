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

import static utils.Constants.TIMER_TOPIC;


public class KafkaHelper implements Serializable {

    public static Map<String, Object> getDefaultKafkaParams(){
        String kafkaIpPort = System.getenv("KAFKA_IP_PORT");
        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "kafka:29092");
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

    public static void ifEqualThenPublishCurrentDate(Object object1, Object object2, Map<String, Object> kafkaParams){
        ifEqualThenPublish(object1, object2, kafkaParams, TIMER_TOPIC, DateTime.now().toString());
    }

    public static void ifEqualThenPublish(Object object1, Object object2, Map<String, Object> kafkaParams, String topic, String message){
        if(object1 != null && object2 != null && object1.equals(object2)){
            Producer<String, String> kafkaProducer = new KafkaProducer(kafkaParams);
            ProducerRecord<String,String> producerRecord = new ProducerRecord(topic, message);
            kafkaProducer.send(producerRecord);
        }
    }

}
