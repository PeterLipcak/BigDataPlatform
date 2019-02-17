package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.QueryResponse;
import utils.KairosDBHelper;

import java.util.*;

//NOT WORKING -- TESTING
public class AnomalyDetectionModel {

    private static final String anomeliesTableName = "anomalies";
    private static final String consumptionsTableName = "consumptions";
    private static final String pattern = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Create Anomaly Detection Model");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);


        HttpClient client = new HttpClient("http://localhost:9090");

        DateTimeFormatter dtf = DateTimeFormat.forPattern(pattern);
        DateTime startDate = dtf.parseDateTime("2015-04-01 00:00:00");
        DateTime endDate = dtf.parseDateTime("2015-05-01 23:59:59");

        Map<DateTime, List<DataPoint>> dataPointsMap = new HashMap<>();
        int days = 5;

        while(startDate.isBefore(endDate)){
            System.out.println("Number of keys: " + dataPointsMap.size());
            removeOldEntries(dataPointsMap, startDate.minusDays(days-1));
            System.out.println("Number of keys after delete: " + dataPointsMap.size());

            List<DataPoint> oneDayDataPoints = new ArrayList<>();
            QueryBuilder builder = QueryBuilder.getInstance();
            builder
                    .setStart(startDate.toDate())
                    .setEnd(startDate.plusDays(days).toDate())
                    .addMetric(consumptionsTableName)
                    .addTag("customerId", "1");
            QueryResponse response = client.query(builder);
            oneDayDataPoints.addAll(KairosDBHelper.getDataPointsFromResponse(response, consumptionsTableName));
            System.out.println("Number of records: " + oneDayDataPoints.size());

            if(oneDayDataPoints.size() > 0){
                dataPointsMap.put(startDate, oneDayDataPoints);
            }

            ////DO CALCULATIONS
            List<DataPoint> allDataPoints = new ArrayList<>();
            for(List<DataPoint> dateDataPoints : dataPointsMap.values()){
                allDataPoints.addAll(dateDataPoints);
            }

            JavaRDD<DataPoint> dataPointsRdd = ctx.parallelize(allDataPoints);
//            dataPointsRdd.mapToPair()

            startDate = startDate.plusDays(1);
        }


        ctx.stop();
    }

    public static void removeOldEntries(Map<DateTime, List<DataPoint>> map, DateTime lowestDate){
        List<DateTime> datesToBeremoved = new ArrayList<>();
        for(DateTime dateOfMeasurment : map.keySet()){
            if(dateOfMeasurment.isBefore(lowestDate))datesToBeremoved.add(dateOfMeasurment);
        }

        for(DateTime dateToRemove : datesToBeremoved){
            map.remove(dateToRemove);
        }
    }

}
