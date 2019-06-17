package sparkML.src.recommendation

import org.apache.spark.{sql, SparkConf}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  */
object FeatureExtraction {

  val spark = SparkSession.builder.master("local[2]").appName("FeatureExtraction").getOrCreate()

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    //val fields = str.split("\t")
    val fields = str.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  /**
    * In earlier versions of spark, spark context was entry point for Spark. As RDD was main API, it was created and manipulated using context API’s.
    * For every other API,we needed to use different contexts.For streaming, we needed StreamingContext, for SQL sqlContext and for hive HiveContext.
    * But as DataSet and Dataframe API’s are becoming new standard API’s we need an entry point build for them.
    * So in Spark 2.0, we have a new entry point for DataSet and Dataframe API’s called as Spark Session.
    * SparkSession is essentially combination of SQLContext, HiveContext and future StreamingContext.
    * All the API’s available on those contexts are available on spark session also. Spark session internally has a spark context for actual computation.
    */
  def getFeatures(): sql.DataFrame = {
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val baseDir = System.getProperty("user.dir")
    val dataFilePath = baseDir + "/src/main/scala/sparkML/src/recommendation/sample_movielens_ratings.txt"
//    val dataFilePath = baseDir + "/src/main/scala/sparkML/data/ml-100k/u.data"
    val ratings = spark.read.textFile(dataFilePath).map(parseRating).toDF()
    println(ratings.first())

//        val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    //    println(training.first())

    return ratings
  }

  def getSpark(): SparkSession = {
    // silent the INFO log
    spark.sparkContext.setLogLevel("ERROR")
    return spark
  }

  def main(args: Array[String]) {
    getFeatures()
  }

}