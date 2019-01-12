package streaming;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.rdd.CassandraTableScanRDD;
import entities.Consumption;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import java.util.UUID;
import com.datastax.spark.connector.streaming.*;
import utils.Constants;
import utils.KafkaHelper;

import java.util.*;
import java.util.regex.Pattern;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class FromKafkaToCassandra {

    public static void main(String[] args) throws Exception {
        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("FromKafkaToCassandra").set("spark.cassandra.connection.host", "127.0.0.1");
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
            List<Consumption> conptionsList = consums.collect() ;
            for (Consumption consumption : conptionsList) {
                System.out.println(consumption);
            }
            CassandraJavaUtil
                    .javaFunctions(consums)
                    .writerBuilder("platform", "consumptions", mapToRow(Consumption.class))
                    .saveToCassandra();
        });


        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}
