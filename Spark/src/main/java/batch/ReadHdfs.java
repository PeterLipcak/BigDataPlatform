package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.Query;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Result;
import utils.KairosDBHelper;

import java.io.IOException;
import java.util.*;

//NOT WORKING
public class ReadHdfs {

    private static final String anomeliesTableName = "anomalies";
    private static final String consumptionsTableName = "consumptions";

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("ConsumptionsFromHDFS");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> customerIds = ctx.textFile("hdfs://localhost:9000/customerIds.txt");

//        System.out.println(lines.toString());

        customerIds.foreachPartition(customerIdPartitions -> {
            List<DataPoint> dataPoints = new ArrayList<>();
            try {
                HttpClient client = new HttpClient("http://localhost:9090");
                while (customerIdPartitions.hasNext()) {
                    String customerId = customerIdPartitions.next();

                    QueryBuilder builder = QueryBuilder.getInstance();
                    builder.setStart(new DateTime().minusYears(6).toDate())
                            .addMetric(consumptionsTableName)
                            .addTag("customerId", customerId);
                    QueryResponse response = client.query(builder);
                    dataPoints.addAll(KairosDBHelper.getDataPointsFromResponse(response, consumptionsTableName));
                    System.out.println("customerId=" + customerId + "  number of records=" + dataPoints.size());
                }

            } catch (IOException exception) {
                exception.printStackTrace();
            }
        });


        ctx.stop();
    }

}
