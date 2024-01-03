package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ThreadLocalRandom.{current => Rng}


object GrepDataGeneration {

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println("Usage: GrepDataGen <outputDir> <numLines> "
        + "<lineLength> <keyword> <numKeywordOccurrences>");
      System.exit(1)
    }

    val outputDir = args(0)
    val numLines = args(1).toLong
    val lineLength = args(2).toInt
    val keyword = args(3)
    val numKeywordOccurrences = args(4).toLong


    val spark = SparkSession.builder.appName("GrepDataGeneration").getOrCreate()
    import spark.implicits._

    val chars = ((48 to 57)++(65 to 90)++(97 to 122)).map(x => x.toChar)
    val numChars = chars.length

    def randChar() =  chars(Rng.nextInt(numChars))

    val lines = spark.range(numLines)

    val p = 1.0 * numKeywordOccurrences / numLines

    lines
      .map(i => {
        if (Rng.nextDouble < p){
          (1 to lineLength-keyword.length).map(_ => randChar()).mkString + keyword
        }
        else{
          (1 to lineLength).map(_ => randChar()).mkString
        }
      })
      .write.mode("overwrite").text(outputDir)

    spark.stop()
  }
}
