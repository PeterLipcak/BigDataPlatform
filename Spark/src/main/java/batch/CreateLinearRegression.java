package batch;

import entities.Consumption;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.QueryResponse;
import utils.Constants;
import utils.KairosDBHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


//NOT WORKING -- TESTING PURPOSE
public class CreateLinearRegression {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Create Anomaly Detection Model");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        SQLContext sqc = new SQLContext(ctx);

//        sqc.createDataset()

        HttpClient client = new HttpClient("http://localhost:9090");

        DateTimeFormatter dtf = DateTimeFormat.forPattern(Constants.DATE_PATTERN);
        DateTime startDate = dtf.parseDateTime("2015-04-01 00:00:00");
        DateTime endDate = dtf.parseDateTime("2015-05-01 23:59:59");

        int days = 5;

        List<Consumption> dataPoints = new ArrayList<>();
        QueryBuilder builder = QueryBuilder.getInstance();
        builder
                .setStart(startDate.toDate())
                .setEnd(startDate.plusDays(days).toDate())
                .addMetric(Constants.CONSUMPTIONS_TABLE_NAME)
                .addTag("customerId", "1");
        QueryResponse response = client.query(builder);
        dataPoints.addAll(KairosDBHelper.getConsumptionsFromResponse(response, Constants.CONSUMPTIONS_TABLE_NAME, 1));




// Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
//        StructField fieldHourOfDay = DataTypes.createStructField("hourOfDay", DataTypes.IntegerType, true);
        StructField fieldId = DataTypes.createStructField("id", DataTypes.IntegerType, true);
        StructField fieldHourOfDay = DataTypes.createStructField("hourOfDay", DataTypes.IntegerType, true);
//        StructField fieldHourOfDay = new StructField("hourOfDay", new VectorUDT(), true, Metadata.empty());
                StructField fieldConsumptionValie = DataTypes.createStructField("consumptionValue", DataTypes.DoubleType, true);
        fields.add(fieldId);
        fields.add(fieldHourOfDay);
        fields.add(fieldConsumptionValie);
        StructType schema = DataTypes.createStructType(fields);

// Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = ctx.parallelize(dataPoints).map((Function<Consumption, Row>) record -> {
            return RowFactory.create(record.getId(), record.getMeasurementTimestamp().getHours(), record.getConsumption());
        });

// Apply the schema to the RDD
        Dataset<Row> dataPointsDataFrame = sqc.createDataFrame(rowRDD, schema);

        RFormula formula = new RFormula()
                .setFormula("consumptionValue ~ id + hourOfDay")
                .setFeaturesCol("features")
                .setLabelCol("label");

        dataPointsDataFrame.printSchema();
        dataPointsDataFrame.show();

        Dataset<Row> output = formula.fit(dataPointsDataFrame).transform(dataPointsDataFrame);
        output.show();
        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);
//                .setFeaturesCol("hourOfDay");
//
//
//// Fit the model.
        LinearRegressionModel lrModel = lr.fit(output);

// Print the coefficients and intercept for linear regression.
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
//
// Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        trainingSummary.residuals().show(100);
        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
        System.out.println("r2: " + trainingSummary.r2());
    }
}
