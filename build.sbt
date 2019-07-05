
name := "MyMachineLearningExamples"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided"
libraryDependencies += "io.projectreactor" %% "reactor-scala-extensions" % "0.4.0"


// sparkML.src.movies dependency
libraryDependencies += "com.github.wookietreiber" %% "scala-chart" % "latest.integration"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"
libraryDependencies +="org.jfree" % "jfreechart" % "1.0.14"


// sparkML.src.recommendation dependency
libraryDependencies += "org.jblas" % "jblas" % "1.2.4"
libraryDependencies += "com.github.scopt" % "scopt_2.10" % "3.2.0"


// bahirExamples.streaming.zeromq depenency
//libraryDependencies += "org.zeromq" % "jeromq" % "0.5.1"
//libraryDependencies += "org.apache.bahir" %% "spark-streaming-zeromq" % "2.3.3"

libraryDependencies += "io.rsocket" % "rsocket-core" % "0.11.21"
libraryDependencies += "io.rsocket" % "rsocket-transport-netty" % "0.11.21"

libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"


//val circeVersion = "0.10.0"
//
//libraryDependencies ++= Seq(
//  "io.circe" %% "circe-core",
//  "io.circe" %% "circe-generic",
//  "io.circe" %% "circe-parser"
//).map(_ % circeVersion)
