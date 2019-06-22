package sparkExamples.streaming

import java.io.PrintWriter
import java.net.ServerSocket
import java.text.SimpleDateFormat
import java.util.Date


object CustomServer {

  def serveClientConnection(socket : java.net.Socket): Unit = {
    val t = new Thread() {
      override def run = {
        val out = new PrintWriter(socket.getOutputStream(), true)

        while(true) {
          Thread.sleep(1000)
          for(i <- 1 to 10) {
            out.write(i.toString())
          }
          out.write("\n")
          out.flush()
        }
        socket.close()
      }
    }
    t.setDaemon(true)
    t.start()
  }

  def main(args: Array[String]): Unit = {

    val listener = new ServerSocket(9999)
    println("Listening on port 9999")

    while(true) {
      val socket = listener.accept()
      serveClientConnection(socket)
    }
  }

}
