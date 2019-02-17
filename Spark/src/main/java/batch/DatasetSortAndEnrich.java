package batch;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import utils.Constants;

import static org.apache.hadoop.hdfs.server.namenode.ListPathsServlet.df;
import static org.apache.spark.sql.functions.*;

public class DatasetSortAndEnrich {

    private static final DateTimeFormatter dtf = DateTimeFormat.forPattern(Constants.DATE_PATTERN);

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Consumptions From HDFS")
                .getOrCreate();

        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("inferSchema","true")
                .csv("hdfs://localhost:9000/consumptions/datasetFull.txt")
                .toDF("id","timestamp","consumption");

        dataset = dataset.withColumn("hourOfDay", hour(dataset.col("timestamp")));
        dataset = dataset.withColumn("weekOfYear", weekofyear(dataset.col("timestamp")));
        dataset = dataset.withColumn("monthOfYear", month(dataset.col("timestamp")));
//        dataset = dataset.withColumn("unixTimestamp", unix_timestamp(dataset.col("timestamp")).cast("timestamp"))
        dataset = dataset.orderBy("unixTimestamp");

        dataset.show(100);
        dataset.coalesce(1).write().format("csv").save("hdfs://localhost:9000/consumptions/dataset-sorted");

        spark.stop();
    }

}
