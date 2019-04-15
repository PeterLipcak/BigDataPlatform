export const BLANK_FLINK_TEMPLATE = `import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import scala.Tuple2;
import scala.Tuple3;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Main {

    private static String kafkaUri;
    private static String zookeeperUri;
    private static String hdfsUri;

    public static void main(String[] args) throws Exception {
        initEnvironmentVariables();
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final double limit = 5;

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaUri);
        properties.setProperty("zookeeper.connect", zookeeperUri);
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumptions = new FlinkKafkaConsumer<>("consumptions", new SimpleStringSchema(), properties);

        DataStream<String> stream = env
                .addSource(consumptions);

        FlinkKafkaProducer<String> anomalies = new FlinkKafkaProducer<String>(
                kafkaUri,            // broker list
                "anomalies",                  // target topic
                new SimpleStringSchema());

        stream.rebalance().filter(consumption -> {
            String[] recordSplits = consumption.split(",");
            return Double.parseDouble(recordSplits[2]) > limit;
          })
                .map(c -> c.toString())
                .addSink(anomalies);

        env.execute();
    }

    public static void initEnvironmentVariables(){
        String kafkaIP = System.getenv("KAFKA_IP_PORT");
        String zookeeperIP = System.getenv("ZOOKEEPER_IP_PORT");
        String hdfsIP = System.getenv("HDFS_IP_PORT");

        kafkaUri = kafkaIP != null ? kafkaIP : "localhost:9092";
        zookeeperUri = zookeeperIP != null ? zookeeperIP : "localhost:2181";
        hdfsUri = hdfsIP != null ? "hdfs://" + hdfsIP : "hdfs://localhost:9000";
    }

}`;

export const DEFAULT_MVN_TEMPLATE = `

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_2.12</artifactId>
            <version>1.7.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-scala_2.12</artifactId>
            <version>1.7.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table_2.12</artifactId>
            <version>1.7.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-twitter_2.12</artifactId>
            <version>1.7.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka_2.12</artifactId>
            <version>1.7.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.8.5</version>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.8.5</version>
        </dependency>

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs-client</artifactId>
            <version>2.8.5</version>
        </dependency>


        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>2.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.12</artifactId>
            <version>2.0.0</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.zookeeper</groupId>
                    <artifactId>zookeeper</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
    
`;
