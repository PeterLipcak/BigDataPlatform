package big.data.analysis.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.Properties;

public class KafkaHelper {

    public static Properties getKafkaProps(){
        Properties props = new Properties();
        props.put("bootstrap.servers", EnvironmentVariablesHelper.getKafkaIpPort());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static Producer<String, String> getKafkaProducer(){
        return new KafkaProducer<>(getKafkaProps());
    }

}
