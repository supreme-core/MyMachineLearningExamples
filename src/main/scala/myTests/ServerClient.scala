package myTests

import java.io._
import java.net._
import java.util.Date
import java.util.concurrent._

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.spark_project.guava.util.concurrent.ThreadFactoryBuilder


object ServerClient {

  def main(args: Array[String]): Unit = {

      val clientAHost = "localhost"
      val clientAPort = 9999
      val clientBHost = "localhost"
      val clientBPort = 8888

      val conf = new SparkConf().setMaster("local[*]").setAppName("Bi-directional streaming socket")
//      conf.set("spark.driver.allowMultipleContexts", "true") // not recommened, may have unexpected result

      // There can be only 1 SparkContext per JVM instance. 1 spark context per job, and it is possible to have multiple context by submitting multiple jobs to spark cluster
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(1))
//      val ssc2 = new StreamingContext(sc, Seconds(2))
//      val ssc3 = StreamingContext.getActiveOrCreate(() => new StreamingContext(conf, Seconds(1)))
      ssc.sparkContext.setLogLevel("ERROR")

      val clientA = new StreamingSocket(ssc, clientAHost, clientAPort, periodicServe)
      clientA.start()
//      val clientB = new StreamingSocket(ssc, clientBHost, clientBPort, customServing)
//      clientB.start()

//      val clientAStream = clientB.remoteStream(clientAHost, clientAPort)
//      val clientBStream = clientA.remoteStream(clientBHost, clientBPort)
//      val clientBStream2 = clientA.remoteStream(clientBHost, clientBPort)

//      clientAStream.print()
//      clientBStream.print()
//      clientBStream2.print()
//
//      ssc.start()
//      ssc.awaitTermination()

    while(true) {
      Thread.sleep(1000);
    }

  }

  def customServing(localHost: String, localPort: Integer, socket: java.net.Socket) : Unit = {
    val out = new PrintWriter(socket.getOutputStream(), true)
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
    val uuid = java.util.UUID.randomUUID.toString

    try {
      while(true) {
        Thread.sleep(1000)
        out.write("Serving data for " + uuid + ". Sent from " + localHost + ":" + localPort)
        out.write("\n")
        out.flush()
      }
    }
    catch {
      case e: java.io.IOException => {}
      case e: InterruptedException => {}
    }
    finally {
      socket.close()
    }
  }

  def periodicServe(localHost: String, localPort: Integer, socket : java.net.Socket) : Unit = {
    val out = new PrintWriter(socket.getOutputStream(), true)
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
    val uuid = java.util.UUID.randomUUID.toString

    val executor = new ScheduledThreadPoolExecutor(2)

    executor.scheduleAtFixedRate(new Thread("Scheduled Thread") {
      setDaemon(true)
      override def run(): Unit = {
        out.write("Beep at " + new java.util.Date() + "\n")
        out.flush()
      }
    }, 1, 2, TimeUnit.SECONDS)

    while(true) {
      Thread.sleep(5000)
    }
  }

}




class StreamingSocket(ssc : StreamingContext, localHost : String, localPort: Integer, callback: (String, Integer, java.net.Socket) => Unit) {

//  val remoteStreamMap = new ConcurrentHashMap[String, ReceiverInputDStream[String]]()

  def start(): Unit = {

    val listener = new ServerSocket()
    listener.bind(new InetSocketAddress(localHost, localPort))
    println("Listening on " + localHost + ":" + localPort)


    var conCount = 0
    new Thread() {
      setDaemon(true)
      override def run : Unit = {
        try {
          while(true) {
            val socket = listener.accept()
            serveIncomingConnection(socket)
            conCount += 1
          }
        }
        catch {
          case e: Exception => {
            println(e)
            if(!listener.isClosed())
              listener.close()
          }
        }
        finally {
          if(!listener.isClosed())
            listener.close()
        }
      }
    }.start()
  }

  def serveIncomingConnection(socket : java.net.Socket) : Unit = {
    val t = new Thread() {
      setDaemon(true)
      override def run = {
        try{
          callback(localHost, localPort, socket)
        }catch {
          case e: InterruptedException => {
            if(!socket.isClosed())
              socket.close()
          }
          case e: Exception => {
            println(e)
            if(!socket.isClosed())
              socket.close()
          }
        }
        finally {
          if(!socket.isClosed())
            socket.close()
        }
      }
    }.start()
  }

  // implement CustomReceiver in the future for the return stream type
  def remoteStream[string](remoteHost: String, remotePort: Integer):  ReceiverInputDStream[String] = {
    val sts = ssc.socketTextStream(remoteHost, remotePort)
//    remoteStreamMap.put(remoteHost +":" + remotePort.toString(), sts)
    return sts
  }

}

