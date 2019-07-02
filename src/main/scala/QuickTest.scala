import org.apache.spark.{SparkConf, SparkContext}

object QuickTest {

  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")
    val sc = new SparkContext(conf)

    val rddl = sc.makeRDD(Array(1,2,3,4,5,6,7))
    rddl.collect().foreach(println)

  }
}

