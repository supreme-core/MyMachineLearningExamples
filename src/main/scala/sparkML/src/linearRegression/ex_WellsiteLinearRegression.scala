package sparkML.src.linearRegression

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sparkML.src.linearRegression.ex_LinearRegression.executeLR


/*

Objectives:

fluid flow prediction based on speed.
Predict fluid flow base on real-time speed reading. Speed
are read from sbc and send over to the server, value of speed
is given to the prediction api to return back a fluid flow value.

Prediction model are trained on existing wellsite data



 */

object ex_WellsiteLinearRegression {

  def main(args: Array[String]): Unit = {
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    spConfig.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val spark = SparkSession
      .builder()
      .appName("Wellsite Linear Regression").config(spConfig)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    executeLR(spark)
    spark.stop()
  }

  def executeLR(spark: SparkSession) {
    val baseDir = System.getProperty("user.dir")
    val dataFilePath1 = baseDir + "/src/main/scala/sparkML/data/Wellsite_data.csv"
    var trainDf = spark.read.format("csv").option("header", "true").load(dataFilePath1)

//    trainDf.show(50, false)
    trainDf.foreach {
      row => println(row)
    }
  }
}
