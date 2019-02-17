package batch;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;
import utils.Constants;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.apache.spark.sql.functions.*;

public class ConsumptionAverageModel {

    private static final DateTimeFormatter dtf = DateTimeFormat.forPattern(Constants.DATE_PATTERN);
    private static DecimalFormat df2 = new DecimalFormat(".##");

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Consumptions From HDFS")
                .getOrCreate();

//        JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());
//        JavaRDD<String> consumptionsCsv = ctx.textFile("hdfs://localhost:9000/consumptions/datasetFull.txt");

        ///LOADING WEATHER DATA
        Dataset<Row> weatherDataset = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
                .csv("hdfs://localhost:9000/weather/weather.csv")
                .toDF("temperature","icon","humidity","visibility","summary","apparentTemperature","pressure","windSpeed","cloudCover","timestamp","windBearing","precipIntensity","dewPoint","precipProbability");
        weatherDataset = weatherDataset.withColumn("hourOfDay", hour(from_unixtime(weatherDataset.col("timestamp"))));
//        weatherDataset = weatherDataset.withColumn("hourOfDay", hour(weatherDataset.col("timestamp")));
        weatherDataset = weatherDataset.withColumn("dayOfYear", dayofyear(from_unixtime(weatherDataset.col("timestamp"))));
        weatherDataset = weatherDataset.withColumn("year", year(from_unixtime(weatherDataset.col("timestamp"))));

        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
                .csv("hdfs://localhost:9000/consumptions/datasetFull.csv")
                .toDF("id","timestamp","consumption"); //,"hourOfDay","weekOfYear","monthOfYear", "unixTimestamp");

        dataset = dataset.withColumn("hourOfDay", hour(dataset.col("timestamp")));
        dataset = dataset.withColumn("dayOfYear", dayofyear(dataset.col("timestamp")));
        dataset = dataset.withColumn("year", year(dataset.col("timestamp")));

        dataset.createOrReplaceTempView("consumptions");
        weatherDataset.createOrReplaceTempView("weather");
        dataset = spark.sqlContext().sql("SELECT c.id, c.hourOfDay, c.dayOfYear, c.year, c.consumption, w.temperature FROM consumptions c LEFT JOIN weather w ON c.hourOfDay=w.hourOfDay AND c.dayOfYear=w.dayOfYear AND c.year=w.year");

        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs1 = dataset.toJavaRDD()
                .mapToPair(row -> createConsumptionTuple(row, 0));
        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs1Reduced = datasetPairs1.reduceByKey((a,b) -> new Tuple2(a._1+b._1,a._2 + b._2));

        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs2 = dataset.toJavaRDD()
                .mapToPair(row -> createConsumptionTuple(row, 1));
        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs2Reduced = datasetPairs2.reduceByKey((a,b) -> new Tuple2(a._1+b._1,a._2 + b._2));

        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs3 = dataset.toJavaRDD()
                .mapToPair(row -> createConsumptionTuple(row, 2));
        JavaPairRDD<String,Tuple2<Double,Integer>> datasetPairs3Reduced = datasetPairs3.reduceByKey((a,b) -> new Tuple2(a._1+b._1,a._2 + b._2));

        JavaPairRDD<String,Tuple2<Double,Integer>> finalDatasedPaired = datasetPairs1Reduced.union(datasetPairs2Reduced).union(datasetPairs3Reduced);

        JavaPairRDD<String,Tuple2<Double,Integer>> finalDatasedPairedReduced = finalDatasedPaired.reduceByKey((a,b) -> new Tuple2(a._1+b._1,a._2 + b._2));
//        finalDatasedPairedReduced.foreach(a -> System.out.println(a._1 + " consumptions=" + a._2.getKey() + "  num=" + a._2.getValue()));


        JavaRDD<String> expectedConsumptions= finalDatasedPairedReduced.map(a -> a._1 + "," + df2.format(a._2._1/a._2._2));

//        spark
//                .sqlContext()
//                .createDataset(expectedConsumptions.rdd(), Encoders.bean(Row.class))
//                .write()
//                .mode(SaveMode.Overwrite)
//                .csv("hdfs://localhost:9000/consumptions/expected/");

        Date date = new Date();
        expectedConsumptions.saveAsTextFile("hdfs://localhost:9000/consumptions/expected/"+date.getTime());


        spark.stop();
    }

    public static Tuple2<String,Tuple2<Double,Integer>> createConsumptionTuple(Row row, int plusDays){
        int year = row.getAs("year");
        GregorianCalendar cal = new GregorianCalendar();
        boolean isLeapYear = cal.isLeapYear(year);
        int id = row.getAs("id");
        int dayOfYear = (Integer)row.getAs("dayOfYear") + plusDays;
        int hourOfDay = row.getAs("hourOfDay");
        double temperature = fahrenheitToCelsius(row.getAs("temperature"));
        double consumptionMultiplier = 1;
        if(temperature > 24) consumptionMultiplier = 0.8; //COOLING
        if(temperature < 16) consumptionMultiplier = 0.9;  //HEATING
        if(temperature < 5) consumptionMultiplier = 0.8;  //OVERHEATING


        if(isLeapYear){
            if(dayOfYear > 366){
                dayOfYear=dayOfYear-366;
                year++;
            }
        }else{
            if(dayOfYear > 365){
                dayOfYear=dayOfYear-365;
                year++;
            }
        }

        String key = id + "-" + year + "-" + dayOfYear + "-" + hourOfDay;
//        System.out.println(key);

        return new Tuple2(key,new Tuple2((Double)row.getAs("consumption") * consumptionMultiplier,1));
    }

    public static double fahrenheitToCelsius(double fahrenheit){
        return ((fahrenheit - 32)*5)/9;
    }

}
