package berlin.tu.dos.benchspark

import org.apache.spark.sql.{SparkSession, DataFrame}


object Join {

  def main(args: Array[String]): Unit = {

    if (args.length < 3){
      System.err.println("Usage: Join <inputFile1:csv> <inputFile2:csv> <outputDir>")
      System.exit(1)
    }
    Utils.logArgs("Join", args)

    val inputFile1 = args(0)
    val inputFile2 = args(1)
    val outputDir = args(2)

    val spark = SparkSession.builder.appName("Join").getOrCreate()

    // Given column names ("pageId", "userIP", "usedBrowser", "timeStamp"), ("pageId", "pageURL")
    val userVisits = spark.read.format("csv").option("header", "true").load(inputFile1)
    val pages = spark.read.format("csv").option("header", "true").load(inputFile2)

    // Register DataFrames as temporary views
    userVisits.createOrReplaceTempView("userVisits")
    pages.createOrReplaceTempView("webPages")

    // Perform the SQL join using DataFrame API
    val joinedDF = spark.sql("""
      SELECT uv.userIP, wp.pageURL
      FROM userVisits uv
      INNER JOIN webPages wp ON uv.pageId = wp.pageId
    """)

    joinedDF.show()

    joinedDF.write.mode("overwrite")
      .format("csv").option("header", "true")
      .save(outputDir)

    Utils.logRuntime(spark.sparkContext)
    spark.stop()
  }
}
