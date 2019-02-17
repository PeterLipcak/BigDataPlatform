package batch;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.weekofyear;

public class BatchAnomalyDetection {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Consumptions From HDFS")
                .getOrCreate();

        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
                .csv("hdfs://localhost:9000/consumptions/datasetMillion.txt")
                .toDF("id","timestamp","consumption");

        dataset = dataset.withColumn("hourOfDay", hour(dataset.col("timestamp")));
//        dataset = dataset.withColumn("weekOfYear", weekofyear(dataset.col("timestamp")));
//        dataset = dataset.withColumn("monthOfYear", month(dataset.col("timestamp")));

//        VectorAssembler featureAssembler = new VectorAssembler().setInputCols(new String[]{"hourOfDay", "weekOfYear", "monthOfYear"}).setOutputCol("independentFeatures");
        VectorAssembler featureAssembler = new VectorAssembler().setInputCols(new String[]{"hourOfDay"}).setOutputCol("independentFeatures");
        Dataset<Row> datasetWithFeatures = featureAssembler.transform(dataset);
        Dataset<Row> finalizedDataset = datasetWithFeatures.select("independentFeatures", "consumption");

        LinearRegressionModel lrm = LinearRegressionModel.load("hdfs://localhost:9000/consumptions/linear-regression-model/");

        lrm.transform(finalizedDataset).select("consumption","prediction").coalesce(1).write().format("csv").save("hdfs://localhost:9000/consumptions/predictedHourOfDay");

//        lrm.evaluate(finalizedDataset).predictions().select("consumption","prediction").coalesce(1).write().format("csv").save("hdfs://localhost:9000/consumptions/predictedMillion");

        spark.stop();
    }

}
