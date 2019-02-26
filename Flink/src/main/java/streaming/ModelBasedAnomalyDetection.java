package streaming;

import entities.Consumption;
import org.apache.flink.api.common.functions.JoinFunction;
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

public class ModelBasedAnomalyDetection {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

//        String masterIpPort = System.getenv("MASTER_IP_PORT");
        String hdfsIpPort = System.getenv("HDFS_IP_PORT");
        String kafkaIpPort = System.getenv("KAFKA_IP_PORT");
        String zookeeperIpPort = System.getenv("ZOOKEEPER_IP_PORT");


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaIpPort);
        properties.setProperty("zookeeper.connect", zookeeperIpPort);
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> consumptions = new FlinkKafkaConsumer<>("consumptions", new SimpleStringSchema(), properties);

        DataStream<String> kafkaStream = env
                .addSource(consumptions);

//        DataStream<Tuple3<Integer,String,Double>> consumptionTuples = kafkaStream.map(record -> {
//            String[] splits = record.split(",");
//            return new Tuple3(Integer.parseInt(splits[0]), splits[1], Double.parseDouble(splits[2]));
//        });


        FlinkKafkaProducer<String> anomalies = new FlinkKafkaProducer<>(
                "localhost:9092",            // broker list
                "anomalies",                  // target topic
                new SimpleStringSchema());


        DataStreamSource<String> predictedConsumptions = env.readTextFile("hdfs://" + hdfsIpPort + "/consumptions/expected/1550066951732/");

        DataStream<Tuple2<String, Double>> predictedConsumptionTuples = predictedConsumptions.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String record) throws ParseException {
                String[] splits = record.split(",");
                return new Tuple2(splits[0], Double.parseDouble(splits[1]));
            }
        });

//        predictedConsumptionTuples.map(r -> r.toString()).addSink(anomalies);

        DataStream<Tuple4<String, Integer, String, Double>> consumptionsWithCompositeId = kafkaStream.map(new MapFunction<String, Tuple4<String, Integer, String, Double>>() {
            @Override
            public Tuple4<String, Integer, String, Double> map(String row) throws ParseException {
                String[] splits = row.split(",");
                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(splits[1]);
                DateTime consumptionDate = new DateTime(date);
                String compositeId = splits[0] + "-" + consumptionDate.getYear() + "-" + consumptionDate.getDayOfYear() + "-" + consumptionDate.getHourOfDay();
                return new Tuple4(compositeId, Integer.parseInt(splits[0]), splits[1], Double.parseDouble(splits[2]));
            }
        });

//        consumptionsWithCompositeId.map(r -> r.toString()).addSink(anomalies);

        consumptionsWithCompositeId
                .join(predictedConsumptionTuples)
                .where(consumption -> consumption.f0)
                .equalTo(predictedConsumption -> predictedConsumption._1)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .apply(new JoinFunction<Tuple4<String, Integer, String, Double>, Tuple2<String, Double>, Tuple4<Integer, String, Double, Double>>() {
                    @Override
                    public Tuple4<Integer, String, Double, Double> join(Tuple4 first, Tuple2 second) {
                        return new Tuple4(first.f1, first.f2, first.f3, second._2);
                    }
                }).filter(consumption -> consumption.f2 > (consumption.f3*4 + 4)).map(consumptionTuple -> consumptionTuple.toString()).addSink(anomalies);


//        consumptionsWithCompositeId.addSink(anomalies);
//
//
//        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
//        CsvTableSource csvTableSource = new CsvTableSource(
//                "hdfs://127.0.0.1:8020/consumptions/expected/1550066951732/",
//                new String[] { "compositeId", "expectedConsumption" },
//                new TypeInformation<?>[] {
//                        Types.STRING(),
//                        Types.DOUBLE(),
//                });
//
//        tableEnv.registerTableSource("predictedConsumptions", csvTableSource);
//
//        Table predictedC = tableEnv.sqlQuery("SELECT * FROM predictedConsumptions WHERE expectedConsumption>5");


        env.execute();
    }

}
