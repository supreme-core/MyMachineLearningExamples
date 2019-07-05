package myTests

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression._
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import System.nanoTime

import scala.collection.mutable.ArrayBuffer

object basicLinearRegression {

  def main(args: Array[String]) {

    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    spConfig.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val spark = SparkSession
      .builder()
      .appName("basicLinearRegression")
      .config(spConfig)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val baseDir = System.getProperty("user.dir")
    val dataFilePath = baseDir + "/src/main/resources/myTests/regression.csv"

    val schema = StructType(
      Array(StructField("x", FloatType, true), StructField("y", FloatType, true))
    )

    // read it into a csv file
    var df = spark.read.format("csv").option("header", "false").schema(schema).load(dataFilePath)

    // column renamed
    df = df.withColumnRenamed("x", "label")


    // udf function to convert column value
//    val converttUDF: UserDefinedFunction = udf((col_value:Double) => {
//      Vectors.dense(col_value)
//    })
//    // perform transformation
//    df.withColumn("features", converttUDF(col("features")))
////    df.withColumn("label", converttUDF(col("label")))


    var assembler = new VectorAssembler()
      .setInputCols(Array("y"))
      .setOutputCol("features")

    df = assembler.transform(df)

    // break up the data into two sets
    val preparedData = df.randomSplit(Array(0.5, 0.5))
    val trainDf = preparedData(0)
    val testDf = preparedData(1)

    trainDf.show()
    testDf.show()

    // linear regression ml model
    val lir = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    //
    val model = lir.fit(trainDf)
    // make prediction base on test data
    val fullPredictions = model.transform(testDf).cache()


    val predictions = fullPredictions.select("prediction").rdd.map(row => row(0))
    val labels = fullPredictions.select("label").rdd.map(row => row(0))

    val predictionAndLabel = predictions.zip(labels).collect()

    predictionAndLabel.foreach {
      row => println("(predicted,actual) => " + row)
    }

    spark.stop()

  }

}
