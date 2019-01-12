package streaming;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import entities.Consumption;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;

import java.util.*;
import java.util.regex.Pattern;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class FromKafkaToKairosDB {

        public static void main(String[] args) throws Exception {
            if (args.length < 2) {
                System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                        "  <brokers> is a list of one or more Kafka brokers\n" +
                        "  <topics> is a list of one or more kafka topics to consume from\n\n");
                System.exit(1);
            }

            String brokers = args[0];
            String topics = args[1];

            // Create context with a 2 seconds batch interval
            SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").set("spark.cassandra.connection.host", "127.0.0.1");
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

            Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
            Map<String, Object> kafkaParams = new HashMap<>();
//    kafkaParams.put("metadata.broker.list", brokers);
//    kafkaParams.put("bootstrap.servers", brokers);
//    kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//    kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

            kafkaParams.put("bootstrap.servers", "localhost:9092");
            kafkaParams.put("group.id", "test");
            kafkaParams.put("enable.auto.commit", "true");
            kafkaParams.put("auto.commit.interval.ms", "10000");
            kafkaParams.put("session.timeout.ms", "10000");
            kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
            kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

            // Create direct kafka stream with brokers and topics
            JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

            // Get the lines, split them into words, count the words and print
            JavaDStream<String> lines = messages.map(ConsumerRecord::value);
            JavaDStream<Consumption> consumptions = lines.map(line -> new Consumption(line));

            consumptions.foreachRDD(consums -> {
                List<Consumption> conptionsList = consums.collect() ;
                for (Consumption consumption : conptionsList) {
                    System.out.println(consumption);
                }

                consums.foreachPartition(partitionOfConsumptions -> {
                    HttpClient client = new HttpClient("http://localhost:8080");
                    while (partitionOfConsumptions.hasNext()) {
                        Consumption consumption = partitionOfConsumptions.next();
                        MetricBuilder builder = MetricBuilder.getInstance();
                        builder.addMetric("consumption")
                                .addTag("customerId", String.valueOf(consumption.getId()))
                                .addDataPoint(consumption.getMeasurementTimestamp().getTime(), consumption.getConsumption());
                        client.pushMetrics(builder);
                    }
                });

            });


            // Start the computation
            jssc.start();
            jssc.awaitTermination();
        }




}
