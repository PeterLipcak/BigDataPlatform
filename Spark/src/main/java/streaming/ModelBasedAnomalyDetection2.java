package streaming;

import entities.Consumption;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.from_unixtime;

public class ModelBasedAnomalyDetection2 {

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

        final Consumption firstConsumption = new Consumption("100,2014-10-15 10:45:00,0.0");
        final Consumption lastConsumption = new Consumption("1,2015-12-31 23:59:00,0.715816667");

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

        JavaDStream<Row> records = messages.map(record -> {
            Consumption consumption = new Consumption(record.value());
            KafkaHelper.ifEqualThenPublishCurrentDate(consumption,firstConsumption);
            KafkaHelper.ifEqualThenPublishCurrentDate(consumption,lastConsumption);

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

        Map<String, Double> expectedConsumptions = getExpectedConsumptions();

        records.foreachRDD(recordsRdd -> {
            if(recordsRdd.partitions().size() > 0) {

                StructType schema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("timestamp", DataTypes.StringType, false),
                        DataTypes.createStructField("consumption", DataTypes.DoubleType, false),
                        DataTypes.createStructField("compositeId", DataTypes.StringType, false)
                });

                Dataset<Row> consumptionsDataset = spark.sqlContext().createDataFrame(recordsRdd, schema);

                Dataset<Row> anomalies = consumptionsDataset.filter(row-> {
                    Double expectedConsumption = expectedConsumptions.get(row.get(3));
                    if(((Double)row.get(2) + 7) > expectedConsumption)return true;
                    return false;
                });

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



    private static Map<String, Double> getExpectedConsumptions() throws IOException {
        String hdfsIP = System.getenv("HDFS_IP_PORT");
        String hdfsUri = hdfsIP != null ? "hdfs://" + hdfsIP : "hdfs://localhost:9000";

        Configuration hdfsConf = new Configuration();
        hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        hdfsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hdfsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(hdfsUri + "/consumptions/expected/1550066951732"), hdfsConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        FileStatus[] fileStatus = fs.listStatus(new Path(hdfsUri + "/consumptions/expected/1550066951732"));

        final Map<String, Double> expectedConsumptions = new HashMap<>();
        for (FileStatus status : fileStatus) {
            InputStream is = fs.open(status.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                try {
                    String[] splits = line.split(",");
                    expectedConsumptions.put(splits[0], Double.parseDouble(splits[1]));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
        fs.close();

        return expectedConsumptions;
    }

}
