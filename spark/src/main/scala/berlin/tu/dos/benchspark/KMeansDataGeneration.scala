package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ThreadLocalRandom.{current => Rng}


object KMeansDataGeneration {

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: KMeansDataGen <outputDir> <numClusters>"
        + " <numSamples> <numDimensions> [<labeled:bool=false>]");
      System.exit(1)
    }

    val outputDir = args(0)
    val numClusters = args(1).toInt
    val numSamples = args(2).toLong
    val numDimensions = args(3).toInt
    val labeled = if (args.length > 4) args(4).toBoolean else false

    val spark = SparkSession.builder.appName("KmeansDataGeneration").getOrCreate()
    import spark.implicits._

    val minStd = 1
    val maxStd = 3
    val maxCenter = 10
    val minCenter = -10

    def gaussian(mu: Double, sigma: Double) = Rng.nextGaussian * sigma + mu

    val centers = (1 to numClusters).map(_=>{
      (1 to numDimensions).map(_ =>
        (Rng.nextDouble(minCenter, maxCenter), Rng.nextDouble(minStd, maxStd))
      )
    })

    val points = spark.range(numSamples)

    points
      .map(i=> {
                val designatedCenter = Rng.nextInt(0, numClusters)
                val centerCharacteristics = centers(designatedCenter)
                val features = centerCharacteristics
                  .map {case (mu, sigma) => gaussian(mu, sigma)}
                toLibsvmFormat(if (labeled) designatedCenter else i, features)
      })
      .write.mode("overwrite").text(outputDir)

    spark.sparkContext.stop()
  }

  def toLibsvmFormat(label: Long, features: IndexedSeq[Double]): String = {
    label + " " + features
      .zip(1 to features.length)
      .map(t => t._2.toString + ":" + t._1.toString)
      .mkString(" ")
  }
}
