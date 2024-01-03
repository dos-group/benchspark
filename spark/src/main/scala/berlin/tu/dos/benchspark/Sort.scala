package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession


object Sort {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Sort").getOrCreate()

    if (args.length < 2){
      System.err.println("Usage: Sort <inputFile> <outputDir>")
      System.exit(1)
    }
    Utils.logArgs("Sort", args)

    val inputFile = args(0)
    val outputDir = args(1)

    val text = spark.read.textFile(inputFile)

    val sortedText = text.sort("value")

    sortedText.write.mode("overwrite").text(outputDir)

    Utils.logRuntime(spark.sparkContext)
    spark.stop()

  }

}
