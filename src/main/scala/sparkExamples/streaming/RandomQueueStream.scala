package sparkExamples.streaming


import scala.collection.mutable
import org.apache.spark.rdd.{ParallelCollectionRDD, RDD}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import scala.util.Random


object RandomQueueStream {

  def main(args: Array[String]): Unit = {
    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setMaster("local").setAppName("Random Queue Stream")
    val streamNum = 5
    val streamInterval = 1

    // stream are routinely polled on a 1 second basis.
    val ssc = new StreamingContext(sparkConf, Seconds(streamInterval))
    val queueList = createStreams(ssc, streamNum)
    populateStreams(ssc, queueList)
    ssc.stop()
  }

  def handleRDD(queue_id : String, rdd : RDD[Int]): Unit = {
      println(":" + queue_id + ":")
    // queue items
    rdd.foreach(num => {
        println(num)
      })
  }

  def createStreams(ssc: StreamingContext, num : Integer) : mutable.ListBuffer[mutable.Queue[RDD[Int]]] = {

    val sqlContext = new SQLContext(ssc.sparkContext)
    val spark = sqlContext.sparkSession
    import spark.implicits._

    // create a list to store queues
    val queueList = mutable.ListBuffer[mutable.Queue[RDD[Int]]]()

    // create stream
    for(i <- 0 to num - 1) {

      // each iteration, creates a queue to store rdd[int[]
      val rddQueue = new mutable.Queue[RDD[Int]]
      queueList += rddQueue

      // connect the queue as input to queueStream
      val inputStream = ssc.queueStream(rddQueue)

      // each item inside a queue contains a RDD[Int]
      // this is called as often as you indicate through the streaming context
      inputStream.foreachRDD((rdd, t) => {
        if(!rdd.isEmpty()) {
          if(i == 0) {
            println("----- " + t + " ----")
          }
          handleRDD("queue " + i, rdd)
        }
        else {
//          println("empty " + t)
        }
      })
    }
    ssc.start()

    return queueList
  }


  def populateStreams(ssc: StreamingContext, queueList : mutable.ListBuffer[mutable.Queue[RDD[Int]]]) : Unit = {
    val sqlContext = new SQLContext(ssc.sparkContext)
    var spark = sqlContext.sparkSession

    while(true) {
      for(i <- 0 to queueList.size - 1) {
        val q = queueList(i)

        q.synchronized {
          val leadingNum = i * 10
//          q += spark.sparkContext.parallelize(Array((leadingNum + 1), (leadingNum + 2)))
//          q += spark.sparkContext.parallelize(Array((leadingNum + Random.nextInt(9)), (leadingNum + Random.nextInt(9))))
          q += spark.sparkContext.parallelize(Array((leadingNum + Random.nextInt(9))))

        }
      }

      Thread.sleep(1000)
    }
  }

}
