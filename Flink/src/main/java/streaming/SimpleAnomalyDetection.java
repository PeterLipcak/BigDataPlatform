package streaming;

import entities.Consumption;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SimpleAnomalyDetection {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final Double limit = Double.parseDouble(parameterTool.getRequired("limit"));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumptions = new FlinkKafkaConsumer<>("consumptions", new SimpleStringSchema(), properties);

        DataStream<String> stream = env
                .addSource(consumptions);

        FlinkKafkaProducer<String> anomalies = new FlinkKafkaProducer<String>(
                "localhost:9092",            // broker list
                "anomalies",                  // target topic
                new SimpleStringSchema());

        stream
                .map(kafkaRow -> new Consumption(kafkaRow))
                .rebalance()
                .filter(consumption -> consumption.getConsumption() > limit)
                .map(c -> c.toString())
                .addSink(anomalies);

        env.execute();
    }

}
