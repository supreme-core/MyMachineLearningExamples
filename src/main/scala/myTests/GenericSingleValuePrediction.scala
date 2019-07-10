package myTests

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.SparkConf
import org.apache.spark.ml.PredictionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, FloatType, StructField, StructType}
import org.apache.spark.sql.DataFrame
import sparkML.src.linearRegression.ex_SimpleLR.{getTestData, getTrainData}
import sparkML.src.recommendation.FeatureExtraction.spark

import scala.collection.mutable.ListBuffer


class SingleValueLinearRegressionPrediction {

  private var ss: SparkSession = _
  private var x_colName: String = "X"
  private var y_colName: String = "Y"
  private var model : PredictionModel[Vector, _] = _

  def setDefaultSparkSession(appName: String): SparkSession = {
    val conf = (new SparkConf).setMaster("local").setAppName("SingleValueLinearRegressionPrediction")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    ss = SparkSession
            .builder()
            .appName(appName)
            .config(conf)
            .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    return ss
  }

  // Each line in the CSV file must be in the format of x,y , where x is the feature(input) and y is the label(prediction)
  def readCSV(csvFullPath: String, csvHeaderSet: Boolean) : DataFrame = {
    val schema = StructType(
      Array(StructField(x_colName, DoubleType, true), StructField(y_colName, DoubleType, true))
    )
    var df = ss.read.format("csv").option("header", csvHeaderSet).schema(schema).load(csvFullPath)
    return df
  }

  def prepare(df : DataFrame): DataFrame = {
    var newDf = df.withColumnRenamed(y_colName, "label")
    var assembler = new VectorAssembler()
          .setInputCols(Array(x_colName))
          .setOutputCol("features")
    return assembler.transform(newDf)
  }

  def train(trainDF: DataFrame) : Unit = {
    var lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    model = lr.fit(trainDF)
  }

  def trainAndTest(trainDF : DataFrame, testDF : DataFrame): DataFrame = {
//    var lr = new LinearRegression() // without parameter tuning, predicted value will overfit, useful in the case of consistent relationship
    var lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    model = lr.fit(trainDF)
    return model.transform(testDF)
  }

  def showPrediction(predictedDF : DataFrame): Unit = {
    val predictions = predictedDF.select("prediction").rdd.map(row => row(0))
    val labels = predictedDF.select("label").rdd.map(row => row(0))
    val predictionAndLabel = predictions.zip(labels).collect()
    predictionAndLabel.foreach {
      row => println("(predicted,actual) => " + row)
    }
  }

  def predict(raw_feature: Double): Double = {
    return model.predict(org.apache.spark.ml.linalg.Vectors.dense(raw_feature))
  }

}


object GenericSingleValuePrediction {

  def main(args: Array[String]) : Unit = {
//    trainFromCSVTest()
    trainFromCustomValues()
  }

  def trainFromCSVTest(): Unit = {
    val lrP = new SingleValueLinearRegressionPrediction()
    lrP.setDefaultSparkSession("Prediction App")

    val baseDir = System.getProperty("user.dir")
    val dataFilePath = baseDir + "/src/main/resources/myTests/regression.csv"

    var df = lrP.readCSV(dataFilePath, false)

    df = lrP.prepare(df)
    val data = df.randomSplit(Array(0.5, 0.5))
    val trainDF = data(0)
    val testDF = data(1)

    val predictedDF = lrP.trainAndTest(trainDF, testDF)
    lrP.showPrediction(predictedDF)
  }

  def trainFromCustomValues() : Unit = {
    import spark.implicits._

    val lrP = new SingleValueLinearRegressionPrediction()
    lrP.setDefaultSparkSession("Prediction App")

    // change the indexFactor and scalingFactor as you wish to run different tests
    val indexFactor = 1
    val scalingFactor = 10
    val trainData = getTrainData(indexFactor, scalingFactor)
    val testData = getTestData(indexFactor, scalingFactor)

    var trainDf = trainData.toDF("X", "Y")
    var testDf = testData.toDF("X", "Y")

    var df = trainDf.union(testDf)
    df = lrP.prepare(df)
    val data = df.randomSplit(Array(0.5, 0.5))
    trainDf = data(0)
    testDf = data(1)

    val predictedDF = lrP.trainAndTest(trainDf, testDf)
    lrP.showPrediction(predictedDF)


    val startTime = System.nanoTime()
    for (i <- 1 to 10) {
      val result = lrP.predict(i)
      println("(i,result): " + "(" + i + "," + result + ")")
    }
    println("Took " + (System.nanoTime() - startTime))

  }

  def getTrainData(indexFactor: Integer, scalingFactor: Integer): ListBuffer[(Int, Int)] = {
    val trainData = ListBuffer[(Int, Int)]()
    for (i <- 1 to 100)
      trainData += ((i * indexFactor , i * scalingFactor))
    return trainData
  }

  def getTestData(indexFactor: Integer, scalingFactor: Integer): ListBuffer[(Int, Int)] = {
    val testData = ListBuffer[(Int, Int)]()
    for (i <- 100 to 250)
      testData += ((i * indexFactor, i * scalingFactor))
    //        testData += ((i, i))
    return testData
  }

}


