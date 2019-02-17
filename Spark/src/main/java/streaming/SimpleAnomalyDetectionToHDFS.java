package streaming;

import entities.Consumption;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import utils.Constants;
import utils.KafkaHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SimpleAnomalyDetectionToHDFS {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: SimpleAnomalyDetection <limit>\n" +
                    "  <limit> if consumption exceeds this value then this metric is considered as anomaly\n" +
                    "  <duration> spark streaming duration\n\n");
            System.exit(1);
        }

        System.out.println(Runtime.getRuntime().availableProcessors());

        Integer limit = Integer.parseInt(args[0]);
        Integer duration = Integer.parseInt(args[1]);


        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("SimpleAnomalyDetection").set("spark.cassandra.connection.host", "127.0.0.1");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(duration));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(Constants.METRICS_DATA_TOPIC.split(",")));

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, KafkaHelper.getDefaultKafkaParams()));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        JavaDStream<Consumption> consumptions = lines.map(line -> new Consumption(line));

//        consumptions.foreachRDD(consums -> {
//            consums.foreachPartition(partitionOfConsumptions -> {
//                Producer<String, String> kafkaProducer = new KafkaProducer(KafkaHelper.getDefaultKafkaParams());
//                while (partitionOfConsumptions.hasNext()) {
//                    Consumption consumption = partitionOfConsumptions.next();
//
//                    if (consumption.getConsumption() > limit) {
//                        ProducerRecord<String, String> producerRecord = new ProducerRecord(Constants.ANOMALIES_DATA_TOPIC, consumption.toString());
//                        kafkaProducer.send(producerRecord);
//                    }
//                }
//            });
//
//        });

        consumptions.foreachRDD(consums -> {
            consums.saveAsTextFile("hdfs://localhost:9000/consumptions");
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}
