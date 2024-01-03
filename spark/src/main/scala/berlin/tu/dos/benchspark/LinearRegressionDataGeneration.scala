package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ThreadLocalRandom.{current => Rng}
import scala.math.pow


object LinearRegressionDataGeneration {

  def main(args: Array[String]) = {

    if (args.length != 4 || args(3) != "dense" && args (3) != "libsvm") {
      System.err.println("Usage: LinearRegressionDataGen <outputDir> <numPoints>" +
                         " <numFeatures> (dense|libsvm)")
      System.exit(-1)
    }

    val spark = SparkSession.builder.appName("LinearRegressionDataGeneration").getOrCreate()
    import spark.implicits._

    val outputDir = args(0)
    val numDataPoints = args(1).toLong  // Number of data points
    val numFeatureDimensions = args(2).toInt  // Number of feature dimensions
    val format = args(3)

    def function(x: Double) =  2 * x + 10

    spark.range(numDataPoints)
      .map(_ => {

        val x = Rng.nextDouble()
        val noise = Rng.nextGaussian()

        // Generate the function value with added gaussian noise
        val label = function(x) + noise

        // Generate a vandermatrix from x
        val features = polyvander(x, numFeatureDimensions - 1)

        //toLibsvmFormat(label, features)
        toFormattedString(label, features, format)
      })
    .write.mode("overwrite").text(outputDir)

    spark.stop()
  }

  def toFormattedString(label: Double, features: Array[Double],
                        format: String): String = {
    format match {
      case "dense" => label + "," + features.mkString(" ")
      case "libsvm" => {
        label + " " + features.zip(1 to features.length)
                              .map(t => t._2.toString + ":" + t._1.toString)
                              .mkString(" ")
      }
      case _ => "Invalid format: " + format
    }
  }

  def polyvander(x: Double, order: Int): Array[Double] = {
    (0 to order).map(pow(x, _)).toArray
  }
}
