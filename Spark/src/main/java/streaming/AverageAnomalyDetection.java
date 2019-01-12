package streaming;

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
import org.joda.time.DateTime;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataFormatException;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.Query;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Result;
import utils.KafkaHelper;
import utils.TimeHelper;

import java.io.IOException;
import java.util.*;

import static utils.Constants.METRICS_DATA_TOPIC;
import static utils.KafkaHelper.ifEqualThenPublishCurrentDate;

public class AverageAnomalyDetection {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AverageAnomalyDetection <epsilon> <daysInterval>\n" +
                    "  <epsilon> is a value that average consumption is gonna be multiplied with\n" +
                    "  <daysInterval> integer value that specifies from how many previous days average value should be calculated\n\n");
            System.exit(1);
        }

        System.out.println(Runtime.getRuntime().availableProcessors());

        Integer epsilon = Integer.parseInt(args[0]);
        Integer daysInterval = Integer.parseInt(args[1]);

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("AverageAnomalyDetection").set("spark.cassandra.connection.host", "127.0.0.1");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(METRICS_DATA_TOPIC.split(",")));

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
                        ifEqualThenPublishCurrentDate(consumption.getMeasurementTimestamp(), TimeHelper.getDateFromString("2015-01-01 00:00:00"), KafkaHelper.getDefaultKafkaParams());
                        ifEqualThenPublishCurrentDate(consumption.getMeasurementTimestamp(), TimeHelper.getDateFromString("2015-12-31 23:59:00"), KafkaHelper.getDefaultKafkaParams());

                        double average = getAverageConsumptions(client,consumption,30, daysInterval);
                        System.out.println("AVERAGE: " + average);

                        if(average > 1 && consumption.getConsumption() > average * epsilon){
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

    public static double getAverageConsumptions(HttpClient client, Consumption consumption, int range, int days) throws IOException, DataFormatException {
        int halfRange = range/2;
        List<DataPoint> dataPoints = new ArrayList<>();
        for(int i = 0; i < days; i++) {
            QueryBuilder builder = QueryBuilder.getInstance();
            builder.setStart(new DateTime(consumption.getMeasurementTimestamp()).minusDays(i).minusMinutes(halfRange).toDate())
                    .setEnd(new DateTime(consumption.getMeasurementTimestamp()).minusDays(i).plusMinutes(halfRange).toDate())
                    .addMetric("consumption")
                    .addTag("customerId", String.valueOf(consumption.getId()))
                    .setLimit(60);
            QueryResponse response = client.query(builder);
            dataPoints.addAll(getDataPointsFromResponse(response));
        }

        System.out.println("datapoints length: " + dataPoints.size());

        double sum = 0;
        int count = dataPoints.size();
        for(DataPoint dataPoint : dataPoints){
            sum += dataPoint.doubleValue();
        }

        return count != 0 ? sum/count : -1;
    }

    public static List<DataPoint> getDataPointsFromResponse(QueryResponse response) throws IOException {
        for (Query query : response.getQueries()) {
            for (Result result : query.getResults()) {
                if (result.getName().compareTo("consumption") == 0) {
                    return result.getDataPoints();
                }
            }
        }

        return new ArrayList<>();
    }

}
