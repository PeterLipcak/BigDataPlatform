package batch;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;
import utils.Constants;
import utils.EnvironmentVariablesHelper;

import java.text.DecimalFormat;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of consumption data prediction model
 *
 * @author Peter Lipcak, Masaryk University
 */
public class CreateAnomalyDetectionModel {

    private static DecimalFormat df2 = new DecimalFormat(".##");

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master(EnvironmentVariablesHelper.getSparkIpPort())
                .appName("Consumptions From HDFS")
                .getOrCreate();

        ///LOADING WEATHER DATA
        Dataset<Row> weatherDataset = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
                .csv("hdfs://localhost:9000/datasets/weather.csv")
                .toDF("temperature","icon","humidity","visibility","summary","apparentTemperature","pressure","windSpeed","cloudCover","timestamp","windBearing","precipIntensity","dewPoint","precipProbability");
        weatherDataset = weatherDataset.withColumn("hourOfDay", hour(from_unixtime(weatherDataset.col("timestamp"))));
        weatherDataset = weatherDataset.withColumn("dayOfYear", dayofyear(from_unixtime(weatherDataset.col("timestamp"))));
        weatherDataset = weatherDataset.withColumn("year", year(from_unixtime(weatherDataset.col("timestamp"))));

        //LOADING CONSUMPTION DATA
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
                .csv("hdfs://localhost:9000/datasets/datasetFull.csv")
                .toDF("id","timestamp","consumption"); //,"hourOfDay","weekOfYear","monthOfYear", "unixTimestamp");

        dataset = dataset.withColumn("hourOfDay", hour(dataset.col("timestamp")));
        dataset = dataset.withColumn("dayOfYear", dayofyear(dataset.col("timestamp")));
        dataset = dataset.withColumn("year", year(dataset.col("timestamp")));

        dataset.createOrReplaceTempView("consumptions");
        weatherDataset.createOrReplaceTempView("weather");

        //Join consumption and weather datasets based on year, day of year, and hour of day
        dataset = spark.sqlContext().sql("SELECT c.id, c.hourOfDay, c.dayOfYear, c.year, c.consumption, w.temperature FROM consumptions c LEFT JOIN weather w ON c.hourOfDay=w.hourOfDay AND c.dayOfYear=w.dayOfYear AND c.year=w.year");

        //Duplicate consumption dataset and shift by one day + make average of consumption of one hour
        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs1 = dataset.toJavaRDD()
                .mapToPair(row -> createConsumptionTuple(row, 1));
        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs1Reduced = datasetPairs1.reduceByKey((a,b) -> new Tuple2(a._1+b._1,a._2 + b._2));

        //Duplicate consumption dataset and shift by two days + make average of consumption of one hour
        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs2 = dataset.toJavaRDD()
                .mapToPair(row -> createConsumptionTuple(row, 2));
        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs2Reduced = datasetPairs2.reduceByKey((a,b) -> new Tuple2(a._1+b._1,a._2 + b._2));

        //Duplicate consumption dataset and shift by three days + make average of consumption of one hour
        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs3 = dataset.toJavaRDD()
                .mapToPair(row -> createConsumptionTuple(row, 3));
        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs3Reduced = datasetPairs3.reduceByKey((a,b) -> new Tuple2(a._1+b._1,a._2 + b._2));

        //Unite all consumption datasets
        JavaPairRDD<String,Tuple2<Double,Integer>> finalDatasedPaired = datasetPairs1Reduced.union(datasetPairs2Reduced).union(datasetPairs3Reduced);

        //Make average of all the consumptions from previous three days of specific hour
        JavaPairRDD<String,Tuple2<Double,Integer>> finalDatasedPairedReduced = finalDatasedPaired.reduceByKey((a,b) -> new Tuple2(a._1+b._1,a._2 + b._2));

        //Format the result to contain composite key and expected consumption as csv
        JavaRDD<String> expectedConsumptions= finalDatasedPairedReduced.map(a -> a._1 + "," + df2.format(a._2._1/a._2._2));

        //Write prediction model to HDFS
        Date date = new Date();
        expectedConsumptions.saveAsTextFile("hdfs://localhost:9000/consumptions/expected/"+date.getTime());

        spark.stop();
    }

    /**
     * Create consumption tuple out of row
     * @param row is a consumption row from csv dataset
     * @param minusDays get data
     * @return tuple containing composite key, adjusted consumption value, and value one for aggregation purposes
     */
    public static Tuple2<String,Tuple2<Double,Integer>> createConsumptionTuple(Row row, int minusDays){
        DateTimeFormatter dtf = DateTimeFormat.forPattern("y-D");
        DateTime date = dtf.parseDateTime(row.getAs("year") + "-" + row.getAs("dayOfYear")).minus(minusDays);
        int id = row.getAs("id");
        int hourOfDay = row.getAs("hourOfDay");
        double temperature = fahrenheitToCelsius(row.getAs("temperature"));
        double consumptionMultiplier = 1;

        //Adjust consumption value based on current outside temperature
        if(temperature > 24) consumptionMultiplier = 0.6; //COOLING
        if(temperature < 16) consumptionMultiplier = 0.8;  //HEATING
        if(temperature < 5) consumptionMultiplier = 0.6;  //OVERHEATING

        //Create composite key
        String key = id + "-" + date.getYear() + "-" + date.getDayOfYear() + "-" + hourOfDay;
        return new Tuple2(key,new Tuple2((Double)row.getAs("consumption") * consumptionMultiplier + 7,1));
    }

    /**
     * Transform fahrenheit temperature to celsius
     * @param fahrenheit
     * @return celsius
     */
    public static double fahrenheitToCelsius(double fahrenheit){
        return ((fahrenheit - 32)*5)/9;
    }

}
