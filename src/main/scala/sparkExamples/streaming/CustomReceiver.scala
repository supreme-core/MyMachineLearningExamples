package sparkExamples.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import scala.util.control.NonFatal
import scala.util.control.Breaks._

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver



object CustomReceiver {
  def main(args: Array[String]) {
    val host = "localhost"
    val port = 9999

    val conf = new SparkConf().setMaster("local").setAppName("CustomReceiver")
    conf.set("spark.streaming.backpressure.enabled", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(1))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Find out why receiverStream blocks when reading from socket
    var lines = ssc.receiverStream(new CustomReceiver(host, port))

//    lines.print()
    lines.foreachRDD((rdd)=> {
        rdd.toDF().show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}


class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  private var socket: Socket = _

  def onStart() {

    try {
      socket = new Socket(host, port)
    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }

    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
    print("onStart Completes")
  }


  def onStop() {
    // in case restart thread close it twice
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
      }
    }
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {

      var socket: Socket = null
      var userInput: String = null
      try {
        println(s"Connecting to $host : $port")
        socket = new Socket(host, port)
        println(s"Connected to $host : $port")
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))

        while(!isStopped()) {
          userInput = reader.readLine()
          println(userInput)

          if(userInput != null)
            store(userInput)
          else
            break
        }

        if (!isStopped()) {
          restart("Socket data stream had no more data")
        } else {
        }

      } catch {
        case NonFatal(e) =>
          restart("Error receiving data", e)
      } finally {
        onStop()
      }

  }
}
