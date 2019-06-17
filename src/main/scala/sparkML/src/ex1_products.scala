package sparkML.src

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, SparkSession}


object ex1_products {

  def main(args: Array[String]) {
    val config = new SparkConf().set("spark.streaming.stopGracefullyOnShutdown","true")
//    val sc = new SparkContext("local[2]", "First Spark App", config)
    val sc = new SparkContext("local[2]", "First Spark App", config)
    run(sc)
//    read_csv_test(sc)
  }

  def run(sc: SparkContext) {
    try {
      // silence the INFO logs
//      sc.setLogLevel("ERROR")
      val baseDir = System.getProperty("user.dir")
      val dataFilePath = baseDir + "/src/main/scala/sparkML/data/UserPurchaseHistory.csv"
      println(dataFilePath)

      // we take the raw data in CSV format and convert it into a set of records of the form (user, product, price). Itreutrns MapPartitionsRDD
      val data = sc.textFile(dataFilePath)
        .map(line => line.split(","))
        .map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))

      // let's count the number of purchases
      val numPurchases = data.count()

      // let's count how many unique users made purchases
      val uniqueUsers = data.map { case (user, product, price) => user }.distinct().count()

      // let's sum up our total revenue
      val totalRevenue = data.map { case (user, product, price) => price.toDouble }.sum()

      // let's find our most popular product
      val productsByPopularity = data
        .map { case (user, product, price) => (product, 1) }
        .reduceByKey(_ + _)
        .collect()
        .sortBy(-_._2)
      val mostPopular = productsByPopularity(0)

      // finally, print everything out
      println("Total purchases: " + numPurchases)
      println("Unique users: " + uniqueUsers)
      println("Total revenue: " + totalRevenue)
      println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))
    }
    catch {
      case e: Exception =>
        System.out.println("Exception: " + e.toString)
    }
    finally {
      System.out.println("Stopping...")
      sc.stop()
    }

  }

  def read_csv_test(sc: SparkContext) : Unit = {
    sc.setLogLevel("ERROR")
    val baseDir = System.getProperty("user.dir")
    val dataFilePath = baseDir + "/src/main/scala/pathA/data/UserPurchaseHistory.csv"
    val sqlctx = new SQLContext(sc)
    val df = sqlctx.read.csv(dataFilePath)
    df.show(5)
    // print schema in tree forma
    df.printSchema()
    // select column 1
    df.select("_c0").show()
    // filter column 1 by name 'John'
    df.filter(df("_c0").contains("John")).show()
    sc.stop()
  }
}
