package berlin.tu.dos.benchspark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.desc


object WordCount {

  def main(args: Array[String]): Unit = {

    if (args.length < 2){
      System.err.println("Usage: WordCount <inputFile> <outputDir>")
      System.exit(1)
    }
    Utils.logArgs("WordCount", args)

    val inputFile = args(0)
    val outputDir = args(1)

    val spark = SparkSession.builder.appName("WordCount").getOrCreate()
    import spark.implicits._


    val words = spark.read.textFile(inputFile)
      .groupByKey(identity) // Group words by their value (identity function)
      .count()  // Creates a dataset column named "count(1)", which is an SQL convention
      .sort(desc("count(1)"))

    words.show()

    words.write.mode("overwrite")
      .format("csv").option("header", "true")
      .save(outputDir)

    Utils.logRuntime(spark.sparkContext)
    spark.stop()
  }
}
