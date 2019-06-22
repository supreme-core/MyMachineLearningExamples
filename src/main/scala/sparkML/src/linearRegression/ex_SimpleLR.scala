package sparkML.src.linearRegression

import java.io.File
import java.util
import java.util.{Arrays, List}

import org.apache.commons.io.FileUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, sql}

import scala.collection.mutable.ListBuffer
import reactor.core.scala.publisher.Mono
import sparkML.src.linearRegression.ex_CreditPrediction.run
import sparkML.src.recommendation.FeatureExtraction.spark


object ex_SimpleLR {

  def main(args: Array[String]): Unit = {
    simpleLinearPrediction()
//    runSavedLinearModel()
  }

  def createSparkSession() : SparkSession  = {
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
    .builder()
    .appName("Spark Uni-variable regression").config(spConfig)
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
  }

  def simpleLinearPrediction() : Unit = {
    import spark.implicits._

    val ss = createSparkSession()

    // change the indexFactor and scalingFactor as you wish to run different tests
    val indexFactor = 1
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

    var assembler2 = new VectorAssembler()
        .setInputCols(Array("X"))
        .setOutputCol("features")

    trainDf = assembler.transform(trainDf)
    testDf = assembler.transform(testDf)

    trainDf.show(10, false)
    testDf.show(10, false)

    def modelPredict(trainD : DataFrame, testD : DataFrame): Unit = {
      var lr = new LinearRegression()
      var lrModel = lr.fit(trainD)

      var lrPrediction = lrModel.transform(testD)
      println("======= RESULTS ===========")
      lrPrediction.show(10, false)

      println("Coefficients: " + lrModel.coefficients + " Intercept: " + lrModel.intercept)
      println("Coefficient Standard Errors: " + String.valueOf(lrModel.summary.coefficientStandardErrors.mkString(",")) )

      println("NumFeatures: " + lrModel.numFeatures)
      println("T Values: " + String.valueOf(lrModel.summary.tValues.mkString(",")))
      println("P Values: " + String.valueOf(lrModel.summary.pValues.mkString(",")))
      println("Disperson: " + lrModel.aggregationDepth)
      println("ElasticNetParam: " + lrModel.elasticNetParam)
      println("Epsilon: " + lrModel.epsilon)
      println("FeaturesCol: " + lrModel.featuresCol)
      println("FitIntercept: " + lrModel.fitIntercept)
      println("LabelCol: " + lrModel.labelCol)
      println("Loss: " + lrModel.loss)
      println("MaxIter: " + lrModel.maxIter)
      println("Params: " + lrModel.params)
      println("PredictionCol: " + lrModel.predictionCol)

      val result = lrModel.predict(Vectors.dense(20.0, 106).asML)
      println("Result: " + result)


//      testD.foreach((row) => {
////        val input = util.Arrays.asList(Vectors.dense(20.0, 106.0))
//        val result = lrModel.predict(Vectors.dense(20.0, 106).asML)
//        println(result)
//
//      })

    }

    def pipelinePredict(trainD : DataFrame, testD : DataFrame) : Unit = {
      var lr = new LinearRegression()
      val baseDir = System.getProperty("user.dir")
      val modelFilePath = baseDir + "/src/main/scala/sparkML/data/savedModels/ex_SimpleLR.model"
      val pipelinePath = baseDir + "/src/main/scala/sparkML/data/savedPipelines/ex_SimpleLR_pipeline"
      FileUtils.deleteQuietly(new File(modelFilePath))

      val pipeline = new Pipeline().setStages(Array(lr))
      // Fit the pipeline to training documents.
      val lrModel = pipeline.fit(trainD)
      var lrPrediction = lrModel.transform(testD)
      lrPrediction.show(10, false)

      // save to disk
      lrModel.write.overwrite().save(modelFilePath)
      pipeline.write.overwrite().save(pipelinePath)
      println("Saved pipeline model")

      // load it back
      val sameModel = PipelineModel.load(modelFilePath)
      lrPrediction = sameModel.transform(testD)
      println("--- RESULT ---")
      lrPrediction.show(10, false)
    }


    modelPredict(trainDf, testDf)
//    pipelinePredict(trainDf, testDf)
    ss.stop()

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


  def runSavedLinearModel() : Unit = {

    import spark.implicits._
    import org.apache.spark.ml._

    val testData = getTestData(5, 10)
    var testDf = testData.toDF("X", "Y")

    testDf = testDf.withColumnRenamed("Y", "label")
    var assembler = new VectorAssembler()
      .setInputCols(Array("X", "label"))
      .setOutputCol("features")

    testDf = assembler.transform(testDf)

    val baseDir = System.getProperty("user.dir")
    val modelFilePath = baseDir + "/src/main/scala/sparkML/data/savedModels/ex_SimpleLR.model"
    val pipelinePath = baseDir + "/src/main/scala/sparkML/data/savedPipelines/ex_SimpleLR_pipeline"

    // load it back
    val sameModel = PipelineModel.load(modelFilePath)

    var lrPrediction = sameModel.transform(testDf)
    lrPrediction.show(10, false)

  }

}
