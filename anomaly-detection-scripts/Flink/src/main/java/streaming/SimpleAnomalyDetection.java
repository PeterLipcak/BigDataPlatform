package streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

public class SimpleAnomalyDetection {

    private static String kafkaUri;
    private static String zookeeperUri;

    public static void main(String[] args) throws Exception {
        initEnvironmentVariables();

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final double limit = 5;

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaUri);
        properties.setProperty("zookeeper.connect", zookeeperUri);
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumptions = new FlinkKafkaConsumer<>("consumptions", new SimpleStringSchema(), properties);

        DataStream<String> stream = env
                .addSource(consumptions);

        FlinkKafkaProducer<String> anomalies = new FlinkKafkaProducer<String>(
                kafkaUri,                            // broker list
                "anomalies",                  // target topic
                new SimpleStringSchema());

        stream.rebalance().filter(consumption -> {
            String[] recordSplits = consumption.split(",");
            return Double.parseDouble(recordSplits[2]) > limit;
        })
                .map(c -> c.toString())
                .addSink(anomalies);

        env.execute();
    }

    public static void initEnvironmentVariables(){
        String kafkaIP = System.getenv("KAFKA_IP_PORT");
        String zookeeperIP = System.getenv("ZOOKEEPER_IP_PORT");
        kafkaUri = kafkaIP != null ? kafkaIP : "localhost:9092";
        zookeeperUri = zookeeperIP != null ? zookeeperIP : "localhost:2181";
    }

}