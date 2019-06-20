package sparkML.src.streaming.simple


import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler._

/*

https://github.com/ibm-watson-data-lab/spark.samples/blob/master/streaming-twitter/src/main/scala/com/ibm/cds/spark/samples/MessageHubStreamingTwitter.scala
https://github.com/prithvirajbose/spark-dev/blob/master/src/main/scala/examples/streaming/TestStreamingListener.scala
https://lenadroid.github.io/posts/distributed-data-streaming-action.html


https://github.com/lenadroid?utf8=%E2%9C%93&tab=repositories&q=&type=&language=scala

 */


class MyStreamingListener() extends StreamingListener {

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    println("Batch started")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    println("completed")
  }

//  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = super.onReceiverStarted(receiverStarted)
//  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = super.onReceiverStopped(receiverStopped)
//  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = super.onReceiverError(receiverError)
//  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = super.onBatchSubmitted(batchSubmitted)

}


class Person(val firstName: String, val lastName: String, val age: Int) {

  /**
    * A secondary constructor.
    */
  def this(firstName: String) {
    this(firstName, "", 0);
    println("\nNo last name or age given.")
  }

  /**
    * Another secondary constructor.
    */
  def this(firstName: String, lastName: String) {
    this(firstName, lastName, 0);
    println("\nNo age given.")
  }

  override def toString: String = {
    return "%s %s, age %d".format(firstName, lastName, age)
  }

}


object SinglePageStreaming {

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[2]", "Single Page Streaming", Seconds(1))

    val baseDir = System.getProperty("user.dir")
    val dataFilePath1 = baseDir + "/src/main/scala/sparkML/data/ProductSalesTrain.csv"
    ssc.addStreamingListener(new MyStreamingListener())

    val dstream = ssc.textFileStream(dataFilePath1)
    dstream.print()


    //    dstream.print()
//    ssc.addStreamingListener()
//    var trainDf = spark.read.format("csv").option("header", "true").load(dataFilePath1)
//    ssc.sparkContext.setLogLevel("ERROR")
//    val stream = ssc.socketTextStream("localhost", 9999)


    ssc.start()
    ssc.awaitTermination()
  }
}
