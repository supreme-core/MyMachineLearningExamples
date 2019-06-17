package sparkML.src.streaming.complex

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}



/**
  * A simple streaming linear regression that prints out predicted value for each batch
  */
object StreamingModelConsumer {

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(10))
    ssc.sparkContext.setLogLevel("ERROR")
    val stream = ssc.socketTextStream("localhost", 9999)

    val NumFeatures = 100
    val zeroVector = DenseVector.zeros[Double](NumFeatures)
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1)
      .setStepSize(0.01)

    // create a stream of labeled points
    val labeledStream = stream.map { event =>
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y, features = Vectors.dense(features))
    }

    // train and test model on the stream, and print predictions for illustrative purposes
    model.trainOn(labeledStream)
    //model.predictOn(labeledStream).print()
    model.predictOnValues(labeledStream.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()

  }
}

