package sparkML.src.linearRegression

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

// Prediction of credit balance using linear regression. The data has been crafted to show gradual trend of increasing balance as person's ages goes up.
// The highest age group (segment 5) has the highest credit balance. (Gender, Ethnicity) are not a driving factor and purposely mixed.
object ex_CreditPrediction {

  def main(args: Array[String]): Unit = {
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder()
      .appName("Credit Prediction").config(spConfig)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    run(spark)
    spark.stop()
  }

  def run(sparkSession: SparkSession): Unit = {

    val baseDir = System.getProperty("user.dir")
    val dataFilePath1 = baseDir + "/src/main/scala/sparkML/data/UserCredit.csv"
    var trainDf = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load(dataFilePath1)
    println("TRAIN DATA")
    trainDf.show(10)
    trainDf = trainDf.withColumnRenamed("Balance", "label")

    // convert the categorical columns into indexed columns so the machine learning model can
    // interpret their impact and fit them to the dataset. Tag the categorical column as 'Count' column just like
    // CountVectorizer in Python

    // StringIndexer encodes a string colum of labels to a column of label indices eg. M=0, F=1
    var genderIndexer = new StringIndexer().setInputCol("Gender").setOutputCol("GenderIndexed")
    trainDf = genderIndexer.fit(trainDf).transform(trainDf)

    // OneHotEncoder are similar to StringIndexer, suitable for eliminating bias in categorical data
    var genderEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array(genderIndexer.getOutputCol))
      .setOutputCols(Array("GenderEncoded"))
    trainDf = genderEncoder.fit(trainDf).transform(trainDf)

    var studentIndexer = new StringIndexer().setInputCol("Student").setOutputCol("StudentIndexed")
    trainDf = studentIndexer.fit(trainDf).transform(trainDf)

    var studentEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array(studentIndexer.getOutputCol))
      .setOutputCols(Array("StudentEncoded"))
    trainDf = studentEncoder.fit(trainDf).transform(trainDf)

    var marriedIndexer = new StringIndexer().setInputCol("Married").setOutputCol("MarriedIndexed")
    trainDf = marriedIndexer.fit(trainDf).transform(trainDf)

    var marriedEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array(marriedIndexer.getOutputCol))
      .setOutputCols(Array("MarriedEncoded"))
    trainDf = marriedEncoder.fit(trainDf).transform(trainDf)

    var ethnicityIndexer = new StringIndexer().setInputCol("Ethnicity").setOutputCol("EthnicityIndexed").setHandleInvalid("keep")
    trainDf = ethnicityIndexer.fit(trainDf).transform(trainDf)

    var ethnicityEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array(ethnicityIndexer.getOutputCol))
      .setOutputCols(Array("EthnicityEncoded"))
    trainDf = ethnicityEncoder.fit(trainDf).transform(trainDf)

    // VectorAssembler transforms a combined list of columns into a single vector colum
    // Combining raw features into a single feature vector.
    // Following lines include two versions: One is using LabelEncoder on the columns, and the other one uses OneHotEncoding
    var assembler = new VectorAssembler()
//      .setInputCols(Array("Income", "Limit", "Rating", "Cards", "Age", "Education", "GenderIndexed", "StudentIndexed", "MarriedIndexed", "EthnicityEncoded"))
      .setInputCols(Array("Income", "Limit", "Rating", "Cards", "Age", "Education", "GenderEncoded", "StudentEncoded", "MarriedEncoded", "EthnicityEncoded"))
      .setOutputCol("features")

    trainDf = assembler.transform(trainDf)

    val Array(training, test) = trainDf.randomSplit(Array(0.8, 0.2), seed = 12345)
    var lr = new LinearRegression()
    var lrModel = lr.fit(trainDf)
    var lrPrediction = lrModel.transform(test)

    println("======= RESULTS ===========")
    println("TEST DATA")
    test.show(10)
    println("PREDICT")
    lrPrediction.show(10)
    var r2Evaluator = new RegressionEvaluator().setMetricName("r2")

    // Indicates the positioning of evaluated value whether larger value are the better value
    println("isLargerValueBetter:" + r2Evaluator.isLargerBetter)
    // https://www.bmc.com/blogs/mean-squared-error-r2-and-variance-in-regression-analysis/
    // evaluates model output and returns a scalar value. The lower r2 score shows a low level of correlation, and means a regression model that is only valid in a few cases
    println(r2Evaluator.evaluate(lrPrediction))

  }
}
