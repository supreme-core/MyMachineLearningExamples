package myTests

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object EventCollectionSimpleStreamingApp {

  def main(args: Array[String]) : Unit = {
    val HOST = "localhost"
    val PORT = 9999

    val conf = new SparkConf().setMaster("local[*]").setAppName("VerySimpleStreamingApp")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    val lines = ssc.socketTextStream(HOST, PORT)
//    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}