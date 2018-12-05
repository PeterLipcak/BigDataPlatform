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

import java.io.IOException;
import java.util.*;

import org.joda.time.DateTime;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import utils.Constants;
import utils.KafkaHelper;
import utils.TimeHelper;

import static utils.KafkaHelper.ifEqualThenPublishCurrentDate;

public class SimpleAnomalyDetection {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: SimpleAnomalyDetection <limit>\n" +
                    "  <limit> if consumption exceeds this value then this metric is considered as anomaly\n\n");
            System.exit(1);
        }

        System.out.println(Runtime.getRuntime().availableProcessors());

        Integer limit = Integer.parseInt(args[0]);

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("SimpleAnomalyDetection").set("spark.cassandra.connection.host", "127.0.0.1");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(Constants.METRICS_DATA_TOPIC.split(",")));

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, KafkaHelper.getDefaultKafkaParams()));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        JavaDStream<Consumption> consumptions = lines.map(line -> new Consumption(line));


        consumptions.foreachRDD(consums -> {
            consums.foreachPartition(partitionOfConsumptions -> {
                try {
                    HttpClient client = new HttpClient("http://localhost:8080");
                    while (partitionOfConsumptions.hasNext()) {
                        Consumption consumption = partitionOfConsumptions.next();

                        //for measuring time of processing all data
                        ifEqualThenPublishCurrentDate(consumption.getMeasurementTimestamp(), TimeHelper.getDateFromString("2014-10-15 10:45:00"), KafkaHelper.getDefaultKafkaParams());
                        ifEqualThenPublishCurrentDate(consumption.getMeasurementTimestamp(), TimeHelper.getDateFromString("2015-12-31 23:54:00"), KafkaHelper.getDefaultKafkaParams());

                        if(consumption.getConsumption() > limit){
                            MetricBuilder builder = MetricBuilder.getInstance();
                            builder.addMetric("anomaly")
                                    .addTag("customerId", String.valueOf(consumption.getId()))
                                    .addDataPoint(consumption.getMeasurementTimestamp().getTime(), consumption.getConsumption());
                            client.pushMetrics(builder);
                        }
                    }

                } catch (IOException exception) {
                    exception.printStackTrace();
                }
            });

        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}
