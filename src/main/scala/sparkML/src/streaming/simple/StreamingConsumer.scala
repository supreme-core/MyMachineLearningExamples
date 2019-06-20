package sparkML.src.streaming.simple

import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * A simple Spark Streaming app in Scala
  */
object StreamingConsumer {


  def main(args: Array[String]) {

    val seconds = 1   // change it to speed up or slow down
    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(seconds))
    ssc.sparkContext.setLogLevel("ERROR")
    val stream = ssc.socketTextStream("localhost", 9999)

    printStream(stream)

    ssc.start()
    println("consuming started")
    ssc.awaitTermination()  // blocks until termination

  }

  def printStream(stream : ReceiverInputDStream[String]) {
    // here we simply print out the first few elements of each batch
    // stream.print()
    stream.foreachRDD((rdd, t) => {
      println("****")
      println(rdd.count() + " elements received " + t)
    })
  }


}
