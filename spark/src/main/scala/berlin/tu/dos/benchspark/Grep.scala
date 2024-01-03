package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession


object Grep {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: Grep <inputFile> <keyword> <outDir>")
      System.exit(1)
    }
    Utils.logArgs("Grep", args)

    val inputFile = args(0)
    val keyword = args(1)
    val outputDir = args(2)

    val spark = SparkSession.builder.appName("Grep").getOrCreate()

    val lines = spark.read.textFile(inputFile)

    val result = lines.filter(line => line.contains(keyword))

    result.write.mode("overwrite").text(outputDir)


    Utils.logRuntime(spark.sparkContext)
    spark.stop()

  }
}
