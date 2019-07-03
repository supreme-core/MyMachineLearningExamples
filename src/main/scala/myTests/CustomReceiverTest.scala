package myTests

import java.io.{BufferedReader, FileNotFoundException, InputStream, InputStreamReader, PrintWriter}
import java.net.{InetSocketAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.time.Instant

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


import scala.collection.immutable.Map
import scala.util.control.NonFatal
import scala.util.parsing.json._


case class Person(name: String)


object CustomReceiverTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Custom receiver test")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

//    jsonTest()
//    simpleTest(ssc)
//    socketTest(ssc)
    multiReqSocketTest(ssc)

    ssc.start()
    ssc.awaitTermination()
  }

  def jsonTest() : Unit = {

    val jsonStr = """
      {"name": "Naoki",  "lang": ["Java", "Scala"]}
    """

    val result = JSON.parseFull(jsonStr)
    result match {
        case Some(e) => println(e)
        case None => println("Failed")
    }

    // parse JSON string to scala json object
    val jsonMap = JSON.parseFull(jsonStr).getOrElse(0).asInstanceOf[Map[String, String]]
    println(jsonMap("name"))
    println(jsonMap)

    println("------------------")
    // parse Map object o JSON object
    val states = Map("AL" -> "Alabama", "AK" -> "Alaska")
    val obj = scala.util.parsing.json.JSONObject(states)

    println(obj)            // json object
    println(obj.toString()) // json string

  }

  def simpleTest(ssc : StreamingContext) : Unit = {
      val lines1 = ssc.receiverStream(new StringReceiver())
      val lines2 = ssc.receiverStream(new PersonReceiver())

      lines1.print()
      lines2.print()
  }

  def socketTest(ssc : StreamingContext) : Unit = {

    // TODO: find out why it's having difficult scaling up to more than 3 receiverStream
    val lines = ssc.receiverStream(new SocketReceiver("localhost", 10000))
    val lines2 = ssc.receiverStream(new SocketReceiver("localhost", 10000))
    val lines3 = ssc.receiverStream(new SocketReceiver("localhost", 10000))
    //    val lines4 = ssc.receiverStream(new SocketReceiver("localhost", 10003))
    //    val lines5 = ssc.receiverStream(new SocketReceiver("localhost", 10004))
    //    val lines6 = ssc.receiverStream(new SocketReceiver("localhost", 10005))


    lines.print()
    lines2.print()

    //    lines3.print()
    //    lines4.print()
    //    lines5.print()

  }

  def multiReqSocketTest(ssc : StreamingContext) : Unit = {

    def create_request(topic: String): scala.util.parsing.json.JSONObject = {
      val requestMap = Map("topic" -> topic)
      val requestJSON = scala.util.parsing.json.JSONObject(requestMap)
      return requestJSON
    }

    var requestJSON = create_request("recv1")
    val recv1 = ssc.receiverStream(new RequestSocketReceiver("localhost", 10000, requestJSON.toString()));
    requestJSON = create_request("recv2")
    val recv2 = ssc.receiverStream(new RequestSocketReceiver("localhost", 10000, requestJSON.toString()))
    requestJSON = create_request("recv3")
    val recv3 = ssc.receiverStream(new RequestSocketReceiver("localhost", 10000, requestJSON.toString()))

    recv2.filter((s) => {
      val result = Integer.parseInt(s) > 120 && Integer.parseInt(s) < 150
      result
    })

    recv1.print()
    recv2.print()
    recv3.print()

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

class RequestSocketReceiver(host: String, port: Integer, requestInJSONStr : String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  private var socket: java.net.Socket = _
  private var socket_in : BufferedReader = _
  private var socket_out : PrintWriter = _

  def onStart(): Unit = {
    try {
      socket = new java.net.Socket(host, port)
      socket_out = new PrintWriter(socket.getOutputStream(), true)
      socket_in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
      socket_out.println(requestInJSONStr)
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
    println("OnStop called")
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
        socket_in = null
        socket_out = null
      }
    }
  }

  private def receive(): Unit = {

    try {

      val uuid = java.util.UUID.randomUUID.toString

      while(!isStopped()) {
        if(socket_in.ready()) {
          store(socket_in.readLine())
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


