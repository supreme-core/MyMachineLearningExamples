package sparkML.src.streaming.simple

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * A simple Spark Streaming app in Scala
  */
object StreamingConsumer {
  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(10))
    ssc.sparkContext.setLogLevel("ERROR")
    val stream = ssc.socketTextStream("localhost", 9999)

    // here we simply print out the first few elements of each batch
    stream.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
