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
        Integer duration = Integer.parseInt(args[1]);

        //For measuring proccessing time
        String firstRow = null;
        String lastRow = null;
        boolean shouldMeasureTime = false;
        if(args.length > 2){
            firstRow = args[2];
            lastRow = args[3];
            shouldMeasureTime = true;
        }
        final boolean shouldMeasure = shouldMeasureTime;
        final Consumption firstRowConsumption = shouldMeasure ? new Consumption(firstRow) : null;
        final Consumption lastRowConsumption = shouldMeasure ? new Consumption(lastRow) : null;

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

        consumptions.foreachRDD(consums -> {
            consums.foreachPartition(partitionOfConsumptions -> {
                try {
                    HttpClient client = new HttpClient("http://localhost:9090");
                    while (partitionOfConsumptions.hasNext()) {
                        Consumption consumption = partitionOfConsumptions.next();

                        //for measuring time of processing all data
                        if(shouldMeasure) {
                            ifEqualThenPublishCurrentDate(firstRowConsumption, consumption, KafkaHelper.getDefaultKafkaParams());
                            ifEqualThenPublishCurrentDate(lastRowConsumption, consumption, KafkaHelper.getDefaultKafkaParams());
                        }

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
