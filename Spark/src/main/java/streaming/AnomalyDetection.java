package streaming;

import entities.Consumption;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
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
import utils.Constants;
import utils.KafkaHelper;
import utils.TimeHelper;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class AnomalyDetection {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: SimpleAnomalyDetection <limit>\n" +
                    "  <duration> spark streaming duration\n\n");
            System.exit(1);
        }

        System.out.println(Runtime.getRuntime().availableProcessors());

        Integer duration = Integer.parseInt(args[0]);
        String hdfsIpPort = System.getenv("WEB_HDFS_IP_PORT");

        SparkSession spark = SparkSession.builder()
                .master("spark://spark-master:7077")
                .appName("ModelBasedAnomalyDetection")
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(javaSparkContext, Durations.seconds(duration));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(Constants.CONSUMPTIONS_DATA_TOPIC.split(",")));

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, KafkaHelper.getDefaultKafkaParams()));

        JavaDStream<Row> records = messages.map(record -> {
//            if(record.value().compareTo("100,2014-10-15 10:45:00,0.0") == 0){
//                KafkaHelper.sendToKafka(Constants.TIMER_TOPIC, "FIRST: " + DateTime.now().toString());
//            }else if(record.value().compareTo("1,2015-12-31 23:59:00,0.715816667") == 0){
//                KafkaHelper.sendToKafka(Constants.TIMER_TOPIC, "LAST: " + DateTime.now().toString());
//            }

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

        final Dataset<Row> datasetPredicted = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
//                .csv("hdfs://namenode:8020/consumptions/expected/1550066951732/part*")
                .csv("webhdfs://" + hdfsIpPort + "/datasets/predictions.csv")
                .toDF("compositeId","expectedConsumption");
        datasetPredicted.show(10);
        datasetPredicted.createOrReplaceTempView("predictedConsumptions");

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("timestamp", DataTypes.StringType, false),
                DataTypes.createStructField("consumption", DataTypes.DoubleType, false),
                DataTypes.createStructField("compositeId", DataTypes.StringType, false)
        });
//
//        records.foreachRDD(recordsRdd -> {
//            Dataset<Row> datasetRecords = spark.createDataFrame(recordsRdd, schema);
//            datasetRecords.union(datasetPredicted).withColumn("compositeId", );
//        });
//
//


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

                consumptionsDataset = spark.sqlContext().sql("SELECT c.id, c.timestamp, c.consumption, p.expectedConsumption FROM consumptions c LEFT JOIN predictedConsumptions p ON c.compositeId=p.compositeId");
                consumptionsDataset.createOrReplaceTempView("consumptionsWithPredictions");

                Dataset<Row> anomalies = spark.sqlContext().sql(
                        "SELECT *" +
                                "FROM consumptionsWithPredictions c " +
                                "WHERE c.consumption>(c.expectedConsumption+5)");

                anomalies
                        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                        .writeStream()
                        .format("kafka")
                        .options(KafkaHelper.getDefaultKafkaParamsStringOptions())
                        .option("topic","anomalies")
                        .start();
            }
        });


        jssc.start();
        jssc.awaitTermination();
    }

}
