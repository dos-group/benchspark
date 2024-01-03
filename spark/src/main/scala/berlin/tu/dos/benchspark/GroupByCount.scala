package berlin.tu.dos.benchspark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.desc


object GroupByCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.println("Usage: GroupByCount <inputFile> <outputDir>")
      System.exit(1)
    }
    Utils.logArgs("GroupByCount", args)

    val inputFile = args(0)
    val outputDir = args(1)

    val spark = SparkSession.builder.appName("GroupByCount").getOrCreate()
    import spark.implicits._

    // Given csv column names: "Book", "VerseNumber", "VerseText"
    val verses = spark.read.format("csv").option("header", "true").load(inputFile)
    verses.createOrReplaceTempView("verses")

    val groupings = spark.sql(
      """
      SELECT
        Book,
        COUNT(*) AS mentionsPerBook
      FROM
        verses
      WHERE
        verseText ILIKE '%God%'
        OR verseText ILIKE '%Lord%'
      GROUP BY
        Book
      ORDER BY
        mentionsPerBook DESC;
      """
    )

    groupings.show()

    groupings.write.mode("overwrite")
      .format("csv").option("header", "true")
      .save(outputDir)

    Utils.logRuntime(spark.sparkContext)
    spark.stop()
  }
}
