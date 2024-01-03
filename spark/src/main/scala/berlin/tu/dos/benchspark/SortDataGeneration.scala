package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ThreadLocalRandom.{current => Rng}


object SortDataGeneration {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: SortDataGen <outputDir> <NumLines> <lineLength>");
      System.exit(1)
    }

    val outputDir = args(0)
    val numLines = args(1).toLong
    val lineLength = args(2).toInt

    val spark = SparkSession.builder.appName("SortDataGeneration").getOrCreate()
    import spark.implicits._

    val chars = ((48 to 57)++(97 to 122)++(65 to 90)).map(x => x.toChar)
    val numChars = chars.length

    def randChar() =  chars(Rng.nextInt(numChars))

    val lines = spark.range(numLines)

    lines
      .map(_ => {
        (1 to lineLength).map(_ => randChar()).mkString
      })
      .write.mode("overwrite").text(outputDir)

    spark.stop()
  }
}
