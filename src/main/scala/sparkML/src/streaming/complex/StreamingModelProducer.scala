package sparkML.src.streaming.complex


import java.io.PrintWriter
import java.net.ServerSocket

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
  * A producer application that generates random linear regression data.
  */
object StreamingModelProducer {
  import breeze.linalg._

  def main(args: Array[String]) {

    // Maximum number of events per second
    val MaxEvents = 100
    val NumFeatures = 100

    val random = new Random()

    /** Function to generate a normally distributed dense vector */
    def generateRandomArray(n: Int) = Array.tabulate(n)(_ => random.nextGaussian())

    // Generate a fixed random model weight vector
    val w = new DenseVector(generateRandomArray(NumFeatures))
    val intercept = random.nextGaussian() * 10

    /** Generate a number of random product events */
    def generateNoisyData(n: Int) = {
      (1 to n).map { i =>
        val x = new DenseVector(generateRandomArray(NumFeatures))
        val y: Double = w.dot(x)
        val noisy = y + intercept //+ 0.1 * random.nextGaussian()
        (noisy, x)
      }
    }

    // create a network producer
    val listener = new ServerSocket(9999)
    println("Listening on port: 9999")

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(1000)
            val num = random.nextInt(MaxEvents)
            val data = generateNoisyData(num)
            data.foreach { case (y, x) =>
              val xStr = x.data.mkString(",")
              val eventStr = s"$y\t$xStr"
              out.write(eventStr)
              out.write("\n")
            }
            out.flush()
            println(s"Created $num events...")
          }
          socket.close()
        }
      }.start()
    }
  }
}
