package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ThreadLocalRandom.{current => Rng}


object LogisticRegressionDataGeneration {

  def main(args: Array[String]) = {

    if (args.length != 3) {
      System.err.println("Usage: LogisticRegressionDataGen <outputDir> <numPoints>" +
                         " <numFeatures>")
      System.exit(-1)
    }

    val spark = SparkSession.builder.appName("LogisticRegressionDataGeneration").getOrCreate()
    import spark.implicits._

    val outputDir = args(0)
    val numDataPoints = args(1).toLong  // Number of data points
    val numFeatureDimensions = args(2).toInt  // Number of feature dimensions
    val format = "libsvm"

    spark.range(numDataPoints)
      .map(_ => {

        val y = Rng.nextInt(2)

        val noise = Rng.nextGaussian()
        val x = Array.fill[Double](numFeatureDimensions){
          Rng.nextGaussian + y * noise
        }
        toFormattedString(y, x, format)
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

}
