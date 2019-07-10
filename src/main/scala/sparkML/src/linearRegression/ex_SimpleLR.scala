package sparkML.src.linearRegression

import java.io.File
import java.util

import reactor.core.scala.publisher.Mono
import System.nanoTime

import org.apache.commons.io.FileUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.ml.PredictionModel
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import sparkML.src.linearRegression.ex_CreditPrediction.run
import sparkML.src.recommendation.FeatureExtraction.spark


object ex_SimpleLR {


  def main(args: Array[String]): Unit = {
//    simpleLinearPrediction()
//    simpleLinearPredictionNew()
//    runSavedLinearModel()

//    testList()
//    getMultiFeatureDataTest()

    multiFeaturePrediction()
  }

  def testList() : Unit = {

    val rows = 2
    val cols = 3
    // immutable arra of 2 rows and 3 columns
    val a = Array.ofDim[String](rows, cols)
    println(a)

    // array buffer
    var innerList = scala.collection.mutable.Buffer[String]()
    println(innerList)
    for(i <- 0 to 9) {
      innerList += i.toString()
    }

    println(innerList)
    println(innerList.toList)
    println(innerList.toArray.mkString(","))
    println(innerList.toArray.toList)
    println(innerList.toSeq)
    println("::" + innerList.map(s => (s)))

    var outerList = scala.collection.mutable.ListBuffer[scala.collection.mutable.Buffer[String]]()
    outerList += innerList
    outerList += innerList
    println(outerList)

  }

  def MultiFeatureDataTest() : Unit = {
    val numFeatures = 5
    var indexFactor = 1
    val scalingFactor = 100

    var data = scala.collection.mutable.ListBuffer[scala.collection.mutable.Buffer[Double]]()
    var innerList = scala.collection.mutable.Buffer[Double]()
    println(innerList)

    for(i <- 1 to 10) {
      innerList = scala.collection.mutable.Buffer[Double]()
      var runningTotal = 0
      for(j <- 1 to numFeatures) {
        if(j == 1) {
          runningTotal += indexFactor * i
          innerList += indexFactor * i
        }
        else if(j > 1 && j < numFeatures) {
          runningTotal += j
          innerList += j
        }
        else {
          innerList += runningTotal
        }
      }
      data += innerList
    }
    println(data)
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

  /*
    A feature is a column of the data in your input set.
    A label is the "predicted" final choice
    https://stackoverflow.com/questions/40898019/what-is-the-difference-between-a-feature-and-a-label
   */
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
    testDf = assembler2.transform(testDf)

    trainDf.show(10, false)
    testDf.show(10, false)

    def modelPredict(trainD : DataFrame, testD : DataFrame): Unit = {
      var lr = new LinearRegression()
      var lrModel = lr.fit(trainD)

      var lrPrediction = lrModel.transform(testD)
      println("======= RESULTS ===========")
      lrPrediction.show(10, false)

//      println("Coefficients: " + lrModel.coefficients + " Intercept: " + lrModel.intercept)
//      println("Coefficient Standard Errors: " + String.valueOf(lrModel.summary.coefficientStandardErrors.mkString(",")) )
//
//      println("NumFeatures: " + lrModel.numFeatures)
//      println("T Values: " + String.valueOf(lrModel.summary.tValues.mkString(",")))
//      println("P Values: " + String.valueOf(lrModel.summary.pValues.mkString(",")))
//      println("Disperson: " + lrModel.aggregationDepth)
//      println("ElasticNetParam: " + lrModel.elasticNetParam)
//      println("Epsilon: " + lrModel.epsilon)
//      println("FeaturesCol: " + lrModel.featuresCol)
//      println("FitIntercept: " + lrModel.fitIntercept)
//      println("LabelCol: " + lrModel.labelCol)
//      println("Loss: " + lrModel.loss)
//      println("MaxIter: " + lrModel.maxIter)
//      println("Params: " + lrModel.params)
//      println("PredictionCol: " + lrModel.predictionCol)

//      val result = lrModel.predict(Vectors.dense(20.0, 106).asML)
//      println("Result: " + result)

//      testD.foreach((row) => {
////        val input = util.Arrays.asList(Vectors.dense(20.0, 106.0))
//        val result = lrModel.predict(Vectors.dense(20.0, 106).asML)
//        println(result)
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

//  private var model : PredictionModel[org.apache.spark.ml.linalg.Vector, _] = _

  def simpleLinearPredictionNew() : Unit = {
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

    var assembler2 = new VectorAssembler()
      .setInputCols(Array("X"))
      .setOutputCol("features")

    trainDf = assembler2.transform(trainDf)
    testDf = assembler2.transform(testDf)

    trainDf.show(10, false)
    testDf.show(10, false)

    def modelPredict(trainD : DataFrame, testD : DataFrame): Unit = {
//      var lr = new LinearRegression()
      var lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

      var lrModel = lr.fit(trainD)
//      model = lrModel
//      var lrPrediction = model.transform(testD)
      var lrPrediction = lrModel.transform(testD)


      println("======= RESULTS ===========")
      //      lrPrediction.show(10, false)  // show the whole table after prediction


      // show predicted value verus actual value
      val predictions = lrPrediction.select("prediction").rdd.map(row => row(0))
      val labels = lrPrediction.select("label").rdd.map(row => row(0))
      val predictionAndLabel = predictions.zip(labels).collect()
      predictionAndLabel.foreach {
        row => println("(predicted,actual) => " + row)
      }


      //      println("Coefficients: " + lrModel.coefficients + " Intercept: " + lrModel.intercept)
      //      println("Coefficient Standard Errors: " + String.valueOf(lrModel.summary.coefficientStandardErrors.mkString(",")) )
      //
      //      println("NumFeatures: " + lrModel.numFeatures)
      //      println("T Values: " + String.valueOf(lrModel.summary.tValues.mkString(",")))
      //      println("P Values: " + String.valueOf(lrModel.summary.pValues.mkString(",")))
      //      println("Disperson: " + lrModel.aggregationDepth)
      //      println("ElasticNetParam: " + lrModel.elasticNetParam)
      //      println("Epsilon: " + lrModel.epsilon)
      //      println("FeaturesCol: " + lrModel.featuresCol)
      //      println("FitIntercept: " + lrModel.fitIntercept)
      //      println("LabelCol: " + lrModel.labelCol)
      //      println("Loss: " + lrModel.loss)
      //      println("MaxIter: " + lrModel.maxIter)
      //      println("Params: " + lrModel.params)
      //      println("PredictionCol: " + lrModel.predictionCol)
      //      val result = lrModel.predict(Vectors.dense(20.0, 106).asML)
      //      println("Result: " + result)


      //      testD.foreach((row) => {
      //        //val input = util.Arrays.asList(Vectors.dense(20.0, 106.0))
      //        val result = lrModel.predict(Vectors.dense(20.0, 106).asML)
      //        println(result)
      //
      //      })

      val startTime = System.nanoTime()
      for (i <- 1 to 10) {
        val result = lrModel.predict(org.apache.spark.ml.linalg.Vectors.dense(i))
        println("(i,result): " + "(" + i + "," + result + ")")
      }
      println("Took " + (System.nanoTime() - startTime))
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

  def multiFeaturePrediction() : Unit = {
    import spark.implicits._

    val ss = createSparkSession()

    val indexFactor = 7
    val scalingFactor = 13
    val numFeatures = 10
    val numRows = 100
    val numColumns = numFeatures + 1

    // get raw data rows in the format of (x1, x2, x3 ... xN-1) where N is the numFeatures
    // xN is the corresponding feature, xN-1 is always going to be Y (label)
    // raw data format is in the form of (x1 ...Y)
    val data = getMultiFeatureData(numRows, numFeatures, indexFactor, scalingFactor)

//    data.foreach(item => {
//      println(item.toList)
//    })

    var rows = ListBuffer[Row]()
    // for each row in the training data, convert it to 'row'
    data.foreach(buf => {
      rows += Row(buf.toList: _*)
    })

    val colNames = ListBuffer[String]()
    var structFields = ListBuffer[StructField]()

    // creating as many StructField instance as numFeatures indicates and save them into a ListBuffer
    // StructField are use to define the schema
    for(i <- 1 to numColumns) {
      var colName = ""

      if(i < numColumns) {
        colName = "X" + i.toString()
      }
      else {
        // last column is always Y aka label
        colName = "Y"
      }
      colNames += colName
      structFields += StructField(colName, org.apache.spark.sql.types.DoubleType, true)
    }

    // schema
    val schema = StructType(structFields.toArray)

    // split into two separate data sets
    val dataList = rows.splitAt(49)
    val trainData = dataList._1
    val testData = dataList._2

    var trainDataDF = ss.createDataFrame(spark.sparkContext.parallelize(trainData), StructType(schema))
    var testDataDF = ss.createDataFrame(spark.sparkContext.parallelize(testData), StructType(schema))

    val assembler = new VectorAssembler()
      .setInputCols(colNames.take(colNames.size - 1).toArray)
      .setOutputCol("features")

    trainDataDF = assembler.transform(trainDataDF)
    testDataDF = assembler.transform(testDataDF)

    trainDataDF = trainDataDF.withColumnRenamed("Y", "label")
    testDataDF = testDataDF.withColumnRenamed("Y", "label")

//    trainDataDF.show(10, false)
//    testDataDF.show(10, false)

    var lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    var lrModel = lr.fit(trainDataDF)
    var lrPrediction = lrModel.transform(testDataDF)

//    lrPrediction.show(100)

    val predictions = lrPrediction.select("prediction").rdd.map(row => row(0))
    val labels = lrPrediction.select("label").rdd.map(row => row(0))
    val predictionAndLabel = predictions.zip(labels).collect()
    predictionAndLabel.foreach {
      row => println("(predicted,actual) => " + row)
    }

  }

  def simpleLinearPredictionNewToDelete() : Unit = {
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

    var assembler2 = new VectorAssembler()
      .setInputCols(Array("X"))
      .setOutputCol("features")

    trainDf = assembler2.transform(trainDf)
    testDf = assembler2.transform(testDf)

    trainDf.show(10, false)
    testDf.show(10, false)

    def modelPredict(trainD : DataFrame, testD : DataFrame): Unit = {
      //      var lr = new LinearRegression()
      var lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

      var lrModel = lr.fit(trainD)
      //      model = lrModel
      //      var lrPrediction = model.transform(testD)
      var lrPrediction = lrModel.transform(testD)

      println("======= RESULTS ===========")
      //      lrPrediction.show(10, false)  // show the whole table after prediction

      // show predicted value verus actual value
      val predictions = lrPrediction.select("prediction").rdd.map(row => row(0))
      val labels = lrPrediction.select("label").rdd.map(row => row(0))
      val predictionAndLabel = predictions.zip(labels).collect()
      predictionAndLabel.foreach {
        row => println("(predicted,actual) => " + row)
      }

      //      println("Coefficients: " + lrModel.coefficients + " Intercept: " + lrModel.intercept)
      //      println("Coefficient Standard Errors: " + String.valueOf(lrModel.summary.coefficientStandardErrors.mkString(",")) )
      //
      //      println("NumFeatures: " + lrModel.numFeatures)
      //      println("T Values: " + String.valueOf(lrModel.summary.tValues.mkString(",")))
      //      println("P Values: " + String.valueOf(lrModel.summary.pValues.mkString(",")))
      //      println("Disperson: " + lrModel.aggregationDepth)
      //      println("ElasticNetParam: " + lrModel.elasticNetParam)
      //      println("Epsilon: " + lrModel.epsilon)
      //      println("FeaturesCol: " + lrModel.featuresCol)
      //      println("FitIntercept: " + lrModel.fitIntercept)
      //      println("LabelCol: " + lrModel.labelCol)
      //      println("Loss: " + lrModel.loss)
      //      println("MaxIter: " + lrModel.maxIter)
      //      println("Params: " + lrModel.params)
      //      println("PredictionCol: " + lrModel.predictionCol)
      //      val result = lrModel.predict(Vectors.dense(20.0, 106).asML)
      //      println("Result: " + result)

      //      testD.foreach((row) => {
      //        //val input = util.Arrays.asList(Vectors.dense(20.0, 106.0))
      //        val result = lrModel.predict(Vectors.dense(20.0, 106).asML)
      //        println(result)
      //
      //      })

      val startTime = System.nanoTime()
      for (i <- 1 to 10) {
        val result = lrModel.predict(org.apache.spark.ml.linalg.Vectors.dense(i))
        println("(i,result): " + "(" + i + "," + result + ")")
      }
      println("Took " + (System.nanoTime() - startTime))
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
    // pipelinePredict(trainDf, testDf)
    ss.stop()

  }

  def getMultiFeatureData(numRows: Integer, numFeatures : Integer, indexFactor: Integer, scalingFactor: Integer):
      scala.collection.mutable.ListBuffer[scala.collection.mutable.Buffer[Double]] = {

    var data = scala.collection.mutable.ListBuffer[scala.collection.mutable.Buffer[Double]]()
    var innerList = scala.collection.mutable.Buffer[Double]()
    val numColumns = numFeatures + 1

    for(i <- 1 to numRows) {
      innerList = scala.collection.mutable.Buffer[Double]()
      var runningTotal = 0
      for(j <- 1 to numColumns) {
        if(j == 1) {
          runningTotal += indexFactor * i * scalingFactor
          innerList += indexFactor * i * scalingFactor
        }
        else if(j > 1 && j < numColumns) {
          runningTotal += j
          innerList += j
        }
        else {
          innerList += runningTotal
        }
      }
      data += innerList
    }
    return data

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
