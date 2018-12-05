package batch;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import entities.Consumption;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

//NOT WORKING
public class UpdateAnomalyModel {

//    private static String pattern = "yyyy-MM-dd HH:mm:ss";
    private static DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("WordCount").set("spark.cassandra.connection.host", "127.0.0.1");;
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        DateTime date = dtf.parseDateTime("2014-10-01 00:00:00");
        while(true){
            List<com.datastax.spark.connector.japi.CassandraRow> consumptions = javaFunctions(ctx)
                    .cassandraTable("platform", "consumptions")
                    .where("measurement_timestamp >= ?",dtf.print(date))
                    .where("measurement_timestamp <= ?",dtf.print(date.plusMinutes(30)))
                    .collect();

            double sum = 0;
            for(com.datastax.spark.connector.japi.CassandraRow consumption : consumptions){
                sum += consumption.getDouble("consumption");
                System.out.println(consumption.toString());
            }

            if(consumptions.size() == 0)break;

            System.out.println(dtf.print(date) + "  AVG CONSUMPTION:  " + sum/consumptions.size());

//            if(consumptions.size() > 0){
//                CassandraJavaUtil
//                        .javaFunctions()
//                        .writerBuilder("platform", "model", mapToRow(Consumption.class))
//                        .saveToCassandra();
//            }

            date = date.plusMinutes(30);
//            Thread.sleep(5000);
        }



        ctx.stop();
    }

}
