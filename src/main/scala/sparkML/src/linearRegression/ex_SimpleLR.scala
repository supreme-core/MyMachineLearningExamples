package sparkML.src.linearRegression

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, sql}
import sparkML.src.linearRegression.ex_CreditPrediction.run
import sparkML.src.recommendation.FeatureExtraction.spark

import scala.collection.mutable.ListBuffer

object ex_SimpleLR {

  def main(args: Array[String]): Unit = {
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder()
      .appName("Spark Uni-variable regression").config(spConfig)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    simpleLinearPrediction()
    spark.stop()
  }

  def simpleLinearPrediction() : Unit = {
    import spark.implicits._

    // change the indexFactor and scalingFactor as you wish to run different tests
    val indexFactor = 5
    val scalingFactor = 10
    val trainData = getTrainData(indexFactor, scalingFactor)
    val testData = getTestData(indexFactor, scalingFactor)

    var trainDf = trainData.toDF("X", "Y")
    var testDf = testData.toDF("X", "Y")

    trainDf = trainDf.withColumnRenamed("Y", "label")
    testDf = testDf.withColumnRenamed("Y", "label")

    var assembler = new VectorAssembler()
      .setInputCols(Array("X", "label"))
      .setOutputCol("features")

    trainDf = assembler.transform(trainDf)
    testDf = assembler.transform(testDf)

    trainDf.show(10, false)
    testDf.show(10, false)

    var lr = new LinearRegression()
    var lrModel = lr.fit(trainDf)

    var lrPrediction = lrModel.transform(testDf)

    println("======= RESULTS ===========")
    lrPrediction.show(10, false)

    val baseDir = System.getProperty("user.dir")
    val modelFilePath = baseDir + "/src/main/scala/sparkML/data/savedModels/ex_SimpleLR.model"
    FileUtils.deleteQuietly(new File(modelFilePath))
    lrModel.save(modelFilePath)
    println("Model Saved to " + modelFilePath)
  }

  def getTrainData(indexFactor: Integer, scalingFactor: Integer): ListBuffer[(Int, Int)] = {
    val trainData = ListBuffer[(Int, Int)]()
    for (i <- 1 to 100)
      trainData += ((i * indexFactor , i * scalingFactor + 66))
    trainData
  }

  def getTestData(indexFactor: Integer, scalingFactor: Integer): ListBuffer[(Int, Int)] = {
    val testData = ListBuffer[(Int, Int)]()
    for (i <- 100 to 250)
//      testData += ((i * indexFactor, i * scalingFactor + 66))
        testData += ((i, i))
    testData
  }


}
