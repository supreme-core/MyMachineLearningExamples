package sparkML.src.streaming.simple

import java.io.PrintWriter
import java.net.ServerSocket
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
  * A producer application that generates random "product events", up to 5 per second, and sends them over a
  * network connection
  */
object StreamingProducer {

  def main(args: Array[String]) {

    val random = new Random()

    // Maximum number of events per second
    val MaxEvents = 6

    // Read the list of possible names
    val namesResource = this.getClass.getResourceAsStream("/sparkML/streaming/names.csv")

    val names = scala.io.Source.fromInputStream(namesResource)
      .getLines()           // get all lines
      .toList               // put line into list
      .head                 // get the first line
      .split(",")     // split the line into names
      .toSeq                // convert names into a sequence

    // Generate a sequence of possible products
    val products = Seq(
      "iPhone Cover" -> 9.99,
      "Headphones" -> 5.49,
      "Samsung Galaxy Cover" -> 8.95,
      "iPad Cover" -> 7.49
    )

    /** Generate a number of random product events */
    def generateProductEvents(n: Int) = {
      // iterate 1 to n times, as each iteration will randomly choose the product+price and user
      (1 to n).map { i =>
        val (product, price) = products(random.nextInt(products.size))
        val user = random.shuffle(names).head
        (user, product, price)
      }
    }

    def serveClientThread(socket : java.net.Socket) = {
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(1000)
//            val num = random.nextInt(MaxEvents)
            val num = 5
            val productEvents = generateProductEvents(num)
            productEvents.foreach{ event =>
              out.write(event.productIterator.mkString(","))
              out.write("\n")
            }
            out.flush()
            println(s"Created $num events...")
          }
          socket.close()
        }
      }.start()
    }

    def serveClientThreadSingle(socket : java.net.Socket) = {
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(1000)
            val productEvents = generateProductEvents(1)
            productEvents.foreach{ event =>
              out.write(event.productIterator.mkString(","))
              out.write("\n")
            }
            out.flush()
            println(s"Created 1 event...")
          }
          socket.close()
        }
      }.start()
    }

    // create a network producer
    val listener = new ServerSocket(9999)
    println("Listening on port: 9999")
    // for every incoming client connection, starts a new thread to generate random product events
    while (true) {
      val socket = listener.accept()
      serveClientThread(socket)
//      serveClientThreadSingle(socket)
    }
  }
}
