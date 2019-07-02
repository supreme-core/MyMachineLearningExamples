
object ScalaJavaInteropsTest {

  def main(args: Array[String]) : Unit = {
    println("Hello from Scala")

    // Demonstrates
    // 1. calling Java class from Scala
    // 2. exchanging data between Java and Scala
    val p = new Person("Baron", 76)   // creates Scala class instance
    val c = new Company()                         // create a class from Java
    c.add(p)                                      // calling Java class from scala
    p.host(c)                                     // passing java-based variable into Scala method

    // calling Java library from Scala
    println(new java.util.Date())
    println(new java.util.Random().nextDouble())
  }
}

