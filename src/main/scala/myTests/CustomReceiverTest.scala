package myTests

import java.io.{BufferedReader, FileNotFoundException, InputStream, InputStreamReader, PrintWriter}
import java.net.{InetSocketAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.time.Instant

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.util.control.NonFatal

case class Person(name: String)


object CustomReceiverTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Custom receiver test")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

//    val lines = ssc.receiverStream(new StringReceiver())
//    val lines = ssc.receiverStream(new PersonReceiver())


    // TODO: find out why it's having difficult scaling up to more than 3 receiverStream
    val lines = ssc.receiverStream(new SocketReceiver("localhost", 10000))
    val lines2 = ssc.receiverStream(new SocketReceiver("localhost", 10001))
    val lines3 = ssc.receiverStream(new SocketReceiver("localhost", 10002))
//    val lines4 = ssc.receiverStream(new SocketReceiver("localhost", 10003))
//    val lines5 = ssc.receiverStream(new SocketReceiver("localhost", 10004))
//    val lines6 = ssc.receiverStream(new SocketReceiver("localhost", 10005))

    lines.print()
    lines2.print()
//    lines3.print()
//    lines4.print()
//    lines5.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

// https://mapr.com/blog/how-integrate-custom-data-sources-apache-spark/
class StringReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2)  {

    def onStart(): Unit = {
      new Thread() {
        setDaemon(true)
        override def run(): Unit = {
          receive()
        }
      }.start()
    }

  def onStop(): Unit = {

  }

  private def receive(): Unit = {
    while(!isStopped()) {
      store(Instant.now().toString())
      Thread.sleep(100)
    }
  }
}

class PersonReceiver extends Receiver[Person](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart(): Unit = {
    new Thread() {
      setDaemon(true)
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def onStop(): Unit = {

  }

  private def receive(): Unit = {
    val names = Array("John", "Adam", "Rob", "Harry", "Yolanda")
    var count = 0
    while(!isStopped()) {
      store(Person(names(count % names.length)))
      Thread.sleep(100)
      count = count + 1
    }
  }

}

class SocketReceiver(host: String, port: Integer) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  private var socket: java.net.Socket = _

  def onStart(): Unit = {
    try {
      socket = new java.net.Socket(host, port)
    } catch {
      case e: java.net.ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }

    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }

  def onStop(): Unit = {
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
      }
    }
  }

  private def receive(): Unit = {
    try {

      val out = new PrintWriter(socket.getOutputStream(), true)
      val in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
      val uuid = java.util.UUID.randomUUID.toString

      while(!isStopped()) {
        if(in.ready()) {
          store(in.readLine())
        }
      }

      if (!isStopped()) {
        restart("Socket data stream had no more data")
      }
      else {}

    }
    catch {
      case NonFatal(e) =>
        restart("Error receiving data", e)
    }
    finally {
      onStop()
    }
  }

}
