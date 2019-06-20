package sparkExamples


import java.util.Random

import scala.math.exp
import breeze.linalg.{DenseVector, Vector}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sparkML.src.linearRegression.ex_WellsiteLinearRegression.executeLR

/**
  * Logistic regression based classification.
  * Usage: SparkLR [partitions]
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.ml.classification.LogisticRegression.
  */

object SparkLR {
  val N = 10000  // Number of data points
  val D = 10   // Number of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def generateData: Array[DataPoint] = {
    def generatePoint(i: Int): DataPoint = {
      val y = if (i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D) {
        // round to two decimal places
//        BigDecimal(rand.nextGaussian + y * R).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        rand.nextGaussian + y * R
      }
      DataPoint(x, y)
    }
    var a = Array.tabulate(N)(generatePoint)

    a.foreach((dp) => {
      println(dp)
    })

    println(a)
    return a
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use org.apache.spark.ml.classification.LogisticRegression
        |for more conventional use.
      """.stripMargin)
  }


  def main(args: Array[String]) {

    showWarning()

    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    spConfig.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val spark = SparkSession
      .builder()
      .appName("SparkLR").config(spConfig)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val numSlices = if (args.length > 0) args(0).toInt else 2
    val points = spark.sparkContext.parallelize(generateData, numSlices).cache()

    // Initialize w to a random value
    val w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
    println(s"Initial w: $w")

    for (i <- 1 to ITERATIONS) {
      println(s"On iteration $i")
      val gradient = points.map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println(s"Final w: $w")

    spark.stop()
  }
}

// scalastyle:on println
