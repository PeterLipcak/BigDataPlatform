package streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import utils.Constants;
import utils.EnvironmentVariablesHelper;


/**
 * Implementation of model based anomaly detection algorithm
 *
 * @author Peter Lipcak, Masaryk University
 */
public class ModelBasedAnomalyDetection {

    private static Logger LOG = LoggerFactory.getLogger(ModelBasedAnomalyDetection.class);

    private static String kafkaUri = EnvironmentVariablesHelper.getKafkaIpPort();
    private static String zookeeperUri = EnvironmentVariablesHelper.getZookeeperIpPort();
    private static String hdfsUri = EnvironmentVariablesHelper.getHdfsIpPort();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaUri);
        properties.setProperty("zookeeper.connect", zookeeperUri);
        properties.setProperty("group.id", "test");

        //Create Kafka consumer for consumptions data topic
        FlinkKafkaConsumer<String> consumptions = new FlinkKafkaConsumer<>(Constants.CONSUMPTIONS_DATA_TOPIC, new SimpleStringSchema(), properties);

        //Assign source for flink job
        DataStream<String> kafkaStream = env
                .addSource(consumptions);

        //Create anomalies Kafka sink
        FlinkKafkaProducer<String> anomalies = new FlinkKafkaProducer<>(
                Constants.ANOMALIES_DATA_TOPIC,
                new SimpleStringSchema(),
                properties);

        //Load anomaly prediction model
        final Map<String, Double> expectedConsumptions = loadAnomalyPredictionModel();

        //Create tuples with composite id and consumptions data
        DataStream<Tuple4<String, Integer, String, Double>> consumptionsWithCompositeId = kafkaStream.map(new MapFunction<String, Tuple4<String, Integer, String, Double>>() {
            @Override
            public Tuple4<String, Integer, String, Double> map(String row) throws ParseException {
                String[] splits = row.split(",");
                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(splits[1]);
                DateTime consumptionDate = new DateTime(date);
                String compositeId = splits[0] + "-" + consumptionDate.getYear() + "-" + consumptionDate.getDayOfYear() + "-" + consumptionDate.getHourOfDay();
                return new Tuple4(compositeId, Integer.parseInt(splits[0]), splits[1], Double.parseDouble(splits[2]));
            }
        }).setParallelism(3);

        //Filter out anomalies based on condition and send output to anomalies sink
        consumptionsWithCompositeId.rebalance().filter(consumption -> {
            Double expectedConsumption = expectedConsumptions.get(consumption.f0);
            return expectedConsumption != null && expectedConsumption.doubleValue() < consumption.f3;
        }).setParallelism(3).map(c -> c.toString()).rebalance().addSink(anomalies);

        env.execute();
    }


    /**
     * Loads prediction model from HDFS
     * @return map of composite ids and expected consumptions
     * @throws IOException if problem connecting to HDFS
     */
    private static Map<String, Double> loadAnomalyPredictionModel() throws IOException {
        //Set up hdfs configuration
        Configuration hdfsConf = new Configuration();
        hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        hdfsConf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        hdfsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hdfsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        //Get file system of hdfs
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(hdfsUri + "/datasets/predictions.csv"), hdfsConf);
        } catch (Exception e) {
            LOG.info(e.toString());
        }

        //File or directory is accepted
        FileStatus[] fileStatus = fs.listStatus(new Path(hdfsUri + "/datasets/predictions.csv"));

        //Iterate over files in directory or file and append expected consumptions to hashmap
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
