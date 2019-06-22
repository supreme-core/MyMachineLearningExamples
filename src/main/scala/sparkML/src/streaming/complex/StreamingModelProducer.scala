package sparkML.src.streaming.complex


import java.io.PrintWriter
import java.net.ServerSocket

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/*
https://github.com/apache/bahir/blob/master/streaming-akka/src/main/scala/org/apache/spark/streaming/akka/AkkaUtils.scala
https://github.com/becompany/akka-streams-example/tree/master/src/main/scala/ch/becompany
https://github.com/eBay/Spark/tree/master/examples/src/main/scala/org/apache/spark/examples/streaming
https://github.com/lloydmeta/sparkka-streams/tree/master/src/main/scala/com/beachape/sparkka

https://github.com/apache/bahir
https://github.com/apache/bahir/blob/master/streaming-zeromq/examples/src/main/scala/org/apache/spark/examples/streaming/zeromq/ZeroMQWordCount.scala
https://github.com/abhinavg6/spark-queue-stream/blob/master/src/main/java/com/sapient/stream/process/MeetupEventStream.java
https://github.com/jingpeicomp/product-category-predict/blob/master/src/main/java/com/jinpei/product/category/ml/CategoryModel.java


 */



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
