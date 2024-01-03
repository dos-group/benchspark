package berlin.tu.dos.benchspark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.desc


object SelectWhereOrderBy {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.println("Usage: SelectWhereOrderBy <inputFile> <outputDir>")
      System.exit(1)
    }
    Utils.logArgs("SelectWhereOrderBy", args)

    val inputFile = args(0)
    val outputDir = args(1)

    val spark = SparkSession.builder.appName("SelectWhereOrderBy").getOrCreate()
    import spark.implicits._

    // Given csv column names: "id", "userName", "userAge"
    val users = spark.read.format("csv").option("header", "true").load(inputFile)
    users.createOrReplaceTempView("users")

    // SQL query with filtering ("WHERE") and sorting ("ORDER BY")
    val filteredAndSortedDF = spark.sql("""
      SELECT id, userName, userAge
      FROM users
      WHERE userAge >= 70
      ORDER BY userName ASC
    """)

    filteredAndSortedDF.show()

    filteredAndSortedDF.write.mode("overwrite")
      .format("csv").option("header", "true")
      .save(outputDir)


    Utils.logRuntime(spark.sparkContext)
    spark.stop()
  }
}
