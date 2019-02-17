package batch;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import utils.Constants;

import static org.apache.spark.sql.functions.*;

public class CreateConsumptionPredictions {

    private static final DateTimeFormatter dtf = DateTimeFormat.forPattern(Constants.DATE_PATTERN);

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Consumptions From HDFS")
                .getOrCreate();

//        JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());
//        JavaRDD<String> consumptionsCsv = ctx.textFile("hdfs://localhost:9000/consumptions/datasetFull.txt");

        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
                .csv("hdfs://localhost:9000/consumptions/datasetMillion.txt")
                .toDF("id","timestamp","consumption"); //,"hourOfDay","weekOfYear","monthOfYear", "unixTimestamp");

        dataset = dataset.withColumn("hourOfDay", hour(dataset.col("timestamp")));
//        dataset = dataset.withColumn("weekOfYear", weekofyear(dataset.col("timestamp")));
//        dataset = dataset.withColumn("monthOfYear", month(dataset.col("timestamp")));
        dataset.createOrReplaceTempView("consumptions");
//        dataset = dataset.sqlContext().sql("SELECT * FROM consumptions WHERE consumption > 0");

        dataset.show(100);

        VectorAssembler featureAssembler = new VectorAssembler().setInputCols(new String[]{"hourOfDay"}).setOutputCol("independentFeatures");
        Dataset<Row> datasetWithFeatures = featureAssembler.transform(dataset);

        Dataset<Row> finalizedDataset = datasetWithFeatures.select("independentFeatures", "consumption");

        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setFeaturesCol("independentFeatures")
                .setLabelCol("consumption");

        LinearRegressionModel lrm = lr.fit(finalizedDataset);

        System.out.println("Coefficients: "
                + lrm.coefficients() + " Intercept: " + lrm.intercept());
        LinearRegressionTrainingSummary trainingSummary = lrm.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        trainingSummary.residuals().show(100);
        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
        System.out.println("r2: " + trainingSummary.r2());

//        lrm.evaluate(finalizedDataset).predictions().show(1000);

        lrm.write().overwrite().save("hdfs://localhost:9000/consumptions/linear-regression-model/");

        spark.stop();
    }

}
