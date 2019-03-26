package streaming;

import entities.Consumption;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import utils.Constants;
import utils.KafkaHelper;
import utils.TimeHelper;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.from_unixtime;

public class ModelBasedAnomalyDetection {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: SimpleAnomalyDetection <limit>\n" +
                    "  <limit> if consumption exceeds this value then this metric is considered as anomaly\n" +
                    "  <duration> spark streaming duration\n\n");
            System.exit(1);
        }

        System.out.println(Runtime.getRuntime().availableProcessors());

        Integer duration = Integer.parseInt(args[0]);

        String sparkIpPort = System.getenv("SPARK_IP_PORT");
        String hdfsIpPort = System.getenv("WEB_HDFS_IP_PORT");

        SparkSession spark = SparkSession.builder()
//                .master("spark://spark-master:7077")
                .master("spark://" + sparkIpPort)
                .appName("ModelBasedAnomalyDetection")
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(javaSparkContext, Durations.seconds(duration));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(Constants.METRICS_DATA_TOPIC.split(",")));

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, KafkaHelper.getDefaultKafkaParams()));

        // Get the lines, split them into words, count the words and print
        JavaDStream<Row> records = messages.map(record -> {
            String[] splits = record.value().split(",");
            Date date = TimeHelper.getDateFromString(splits[1]);
            DateTime consumptionDate = new DateTime(date);
            String compositeId = splits[0] + "-" + consumptionDate.getYear() + "-" + consumptionDate.getDayOfYear() + "-" + consumptionDate.getHourOfDay();
            return RowFactory.create(
                    Integer.parseInt(splits[0]),
                    splits[1],
                    Double.parseDouble(splits[2]),
                    compositeId
            );
        });

        records.print();

        Dataset<Row> datasetPredicted = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
//                .csv("hdfs://namenode:8020/consumptions/expected/1550066951732/part*")
                .csv("webhdfs://" + hdfsIpPort + "/consumptions/expected/1550066951732/part*")
                .toDF("compositeId","expectedConsumption");
        datasetPredicted.show(10);
        datasetPredicted.createOrReplaceTempView("predictedConsumptions");

        records.foreachRDD(recordsRdd -> {
            if(recordsRdd.partitions().size() > 0) {

                StructType schema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("timestamp", DataTypes.StringType, false),
                        DataTypes.createStructField("consumption", DataTypes.DoubleType, false),
                        DataTypes.createStructField("compositeId", DataTypes.StringType, false)
                });

                Dataset<Row> consumptionsDataset = spark.sqlContext().createDataFrame(recordsRdd, schema);
                consumptionsDataset.createOrReplaceTempView("consumptions");

//            consumptionsDataset.withColumn("result", col("") - col(""));

                consumptionsDataset = spark.sqlContext().sql("SELECT c.id, c.timestamp, c.consumption, p.expectedConsumption FROM consumptions c LEFT JOIN predictedConsumptions p ON c.compositeId=p.compositeId");
                consumptionsDataset.createOrReplaceTempView("consumptionsWithPredictions");

                Dataset<Row> anomalies = spark.sqlContext().sql(
                        "SELECT *" +
                                "FROM consumptionsWithPredictions c " +
                                "WHERE c.consumption>(c.expectedConsumption+1)*4");


                anomalies.foreachPartition(anomaliesPartition -> {
                    Producer<String, String> kafkaProducer = new KafkaProducer(KafkaHelper.getDefaultKafkaParams());
                    while (anomaliesPartition.hasNext()) {
                        ProducerRecord<String, String> producerRecord = new ProducerRecord(Constants.ANOMALIES_DATA_TOPIC, anomaliesPartition.next().toString());
                        kafkaProducer.send(producerRecord);
                    }
                });
            }
        });





        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}
