package streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

/**
 * Implementation of simple anomaly detection algorithm based on static threshold limit
 *
 * @author Peter Lipcak, Masaryk University
 */
public class SimpleAnomalyDetection {

    private static String kafkaUri;
    private static String zookeeperUri;

    public static void main(String[] args) throws Exception {
        initEnvironmentVariables();

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //threshold above which we consider consumptions as anomaly
        final double limit = 5;

        //Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaUri);
        properties.setProperty("zookeeper.connect", zookeeperUri);
        properties.setProperty("group.id", "test");

        //Create Kafka consumer that lsitens for consumption topic
        FlinkKafkaConsumer<String> consumptions = new FlinkKafkaConsumer<>("consumptions", new SimpleStringSchema(), properties);

        //Create data source
        DataStream<String> stream = env
                .addSource(consumptions);

        //Crate Kafka producer for anomalies topic
        FlinkKafkaProducer<String> anomalies = new FlinkKafkaProducer<String>(
                kafkaUri,                            // broker list
                "anomalies",                  // target topic
                new SimpleStringSchema());

        //Filter out anomalies based on threshold condition and asign sink to Kafka anomalies topic
        stream.rebalance().filter(consumption -> {
            String[] recordSplits = consumption.split(",");
            return Double.parseDouble(recordSplits[2]) > limit;
        })
                .map(c -> c.toString())
                .addSink(anomalies);

        env.execute();
    }

    /**
     * Reads environment variables and assign to kafka and zookeeper
     * (default is selected if no env variable available)
     */
    public static void initEnvironmentVariables(){
        String kafkaIP = System.getenv("KAFKA_IP_PORT");
        String zookeeperIP = System.getenv("ZOOKEEPER_IP_PORT");
        kafkaUri = kafkaIP != null ? kafkaIP : "localhost:9092";
        zookeeperUri = zookeeperIP != null ? zookeeperIP : "localhost:2181";
    }

}