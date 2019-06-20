package sparkExamples.streaming


import scala.collection.mutable.Queue
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import sparkML.src.linearRegression.ex_CreditPrediction.run

import scala.collection.mutable


/*

  RDD - Resilient Distributed Datasets. Read-only colleciton of records
  DataFrame - Tabular data organized into named columns.
  DataSet - same as dataframe with additonal feature in querying

 */

object QueueStream {

  def main(args: Array[String]) {

    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setMaster("local").setAppName("QueueStream")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

//    queue_test1()
    queue_test2(ssc)
//    data_test(ssc)
//    stream_test(ssc)
//    rdd_stream(ssc)

    ssc.stop()
  }

  def queue_test1() : Unit = {

    var q = new Queue[String]
    q += "appple"
    q += "kiwi"
    q += "cherry"

    val v = q.dequeue
    println(v)

    val values = q.dequeueAll(!_.isEmpty)
    println(values)

  }

  def queue_test2(ssc: StreamingContext) : Unit = {
    val sqlContext = new SQLContext(ssc.sparkContext)
    val spark = sqlContext.sparkSession
    import spark.implicits._


    val rddQueue = new Queue[RDD[String]]()
    rddQueue += spark.sparkContext.makeRDD(mutable.Seq("Apple", "Orange", "Banana"))
    rddQueue += spark.sparkContext.makeRDD(mutable.Seq("Bike", "Car", "Bus"))
    val values = rddQueue.dequeueAll((!_.isEmpty()))
    println(values.seq)


    case class Fruits(id: Integer, name: String)
    val rddQueue2 = new Queue[RDD[Fruits]]
    rddQueue2 += spark.sparkContext.makeRDD(mutable.Seq(Fruits(1, "Apple"), Fruits(2, "Orange"), Fruits(3, "Banana")))
    val values2 = rddQueue2.dequeueAll(!_.isEmpty())
    values2.take(5).foreach( (rddItem) => {
      rddItem.foreach(println)
    })

  }


  def data_test(ssc: StreamingContext) : Unit = {

    val sqlContext = new SQLContext(ssc.sparkContext)
    val spark = sqlContext.sparkSession
    import spark.implicits._

    // class 'org.apache.spark.sql.Dataset' is the common underlying type
    val rdd = ssc.sparkContext.makeRDD(1 to 30, 10)
    val ds = spark.createDataset(rdd)
    println(rdd.toDF().getClass())
    println(ds.getClass())
    println(ds.toDF().getClass())

    // hacky way of creating dataframe
    val animalsDF = Seq("Bear", "Horse", "Pig", "Tiger", "Lion").toDF("Name")

    // verbose way of creating dataframe
    val fruitData = Seq( Row(1, "Apple"), Row(2, "Orange"), Row(3, "Banana"), Row(4, "Apricot"))
    val fruitSchema =
        List(
          StructField("Id", IntegerType, true),
          StructField("Name", StringType, true)
        )
    val fruitDF = spark.createDataFrame(spark.sparkContext.parallelize(fruitData), StructType(fruitSchema))

    case class FruitCls(Id: Integer, Name: String)
    val fruitData2 = Seq(FruitCls(1, "Apple"), FruitCls(2, "Orange"), FruitCls(3, "Banana"), FruitCls(4, "Apricot"))
    val fruitData2RDD = ssc.sparkContext.makeRDD[FruitCls](fruitData2)
    val fruitDF2 = spark.createDataFrame(List("Apple", "Orange", "Banana").map(Tuple1(_)))

    // add a RDD object into the queue
    val rdd2 = ssc.sparkContext.makeRDD(Array(1, 2, 3, 4, 5))
    var rddQueue = new Queue[RDD[Int]]
    rddQueue.enqueue(rdd2)

    // use sparkContext to help to create rdd
    val arr = Array.range(10, 20)
    val rdd3 = ssc.sparkContext.parallelize(arr)
    rddQueue.enqueue(rdd3)

    // prints out each item at a item
    while(rddQueue.nonEmpty) {
      val v = rddQueue.dequeue
      println("---")
      v.toDF().show()
    }
  }

  def stream_test(ssc: StreamingContext) : Unit = {
    val sqlContext = new SQLContext(ssc.sparkContext)
    val spark = sqlContext.sparkSession
    import spark.implicits._

    val rdd = ssc.sparkContext.makeRDD(Array(1, 2, 3, 4, 5))
    val df = rdd.toDF()

    var rdd2 = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5))
    val df2 = rdd.toDF()
  }


  def rdd_stream(ssc : StreamingContext) : Unit = {
    val sqlContext = new SQLContext(ssc.sparkContext)
    val spark = sqlContext.sparkSession
//    import spark.implicits._

    // Create the queue through which RDDs can be pushed to a QueueInputStream
    val rddQueue = new Queue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)

    // map processing transformer
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.foreachRDD((rdd, t) => {
//    println(sqlContext.createDataFrame(rdd).first())
      sqlContext.createDataFrame(rdd).show()
    })

    ssc.start()

    // Create and push some RDDs into rddQueue
//  for (i <- 1 to 30) {
    while(true) {
      rddQueue.synchronized {
//        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)

        val a = spark.sparkContext.parallelize(Array(1, 2, 3, 4))
//        rddQueue += spark.sparkContext.parallelize(Array(1, 2, 3, 4))
//        println(a)
        rddQueue += a
      }
      Thread.sleep(1000)
    }
  }

}
