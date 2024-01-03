package berlin.tu.dos.benchspark

import org.apache.spark.ml.clustering.{KMeans => KM}
import org.apache.spark.sql.{SparkSession, DataFrame}


object KMeans {

  def main(args: Array[String]): Unit = {

    if (args.length < 2){
      System.err.println("Usage: KMeans <libsvm_format: inputFile>"
        + " <k> [<expectedNumOfFeatures>]")
      System.exit(1)
    }
    Utils.logArgs("KMeans", args)

    val inputFile = args(0)
    val k = args(1).toInt
    val expectedNumFeatures = if (args.length > 2) Option(args(2)) else None

    val spark = SparkSession.builder.appName("KMeans").getOrCreate()

    val dataset = loadData(spark, inputFile, expectedNumFeatures)
    val kmeans = new KM().setK(k).setSeed(1L)
    val model = kmeans.fit(dataset)
    val centers : Array[org.apache.spark.ml.linalg.Vector] = model.clusterCenters

    println("Centers:\n"+centers.mkString("\n"))

    Utils.logRuntime(spark.sparkContext)
    spark.stop()
  }

  def loadData(spark: SparkSession, path: String,
               expectedNumFeatures: Option[String] = None): DataFrame = {

    expectedNumFeatures match {
        case Some(expectedNumFeatures) => spark.read
          .option("numFeatures", expectedNumFeatures.toString)
          .format("libsvm")
          .load(path)
        case None => spark.read.format("libsvm").load(path)
    }
  }
}
