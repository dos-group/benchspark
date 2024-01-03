package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession
import scala.io.Source
import java.util.concurrent.ThreadLocalRandom.{current => Rng}


object GroupByCountDataGeneration {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: GroupByCountDataGen <inputTextFileUrl> <outputDir> <numVerses>");
      System.exit(1)
    }

    // val inputTextFileURL = "https://openbible.com/textfiles/kjv.txt"  // Bible: King James Version
    val inputTextFileURL = args(0)
    val outputDir = args(1)
    val numVerses = args(2).toLong

    val spark = SparkSession.builder.appName("GroupByCountDataGeneration").getOrCreate()
    import spark.implicits._

    val regex = """(.+) ([1-9]+:[1-9]+)\t(.+)""".r

    def readTextFileToVector(url: String): Vector[(String, String, String)] = {

      val verses: Array[String] = {
        if (inputTextFileURL.contains("http")) {
          Source.fromURL(url).getLines().toArray[String]
        } else {
          spark.read.text(inputTextFileURL).map(_.getString(0)).collect()
        }
      }

      verses.map(regex.findFirstMatchIn(_))
            .map(x => x match {
                case Some(x) => (x.group(1), x.group(2), x.group(3))
                case None => ("", "", "")
              })
            .filter(x => x != ("", "", ""))
            .toVector // Vector(("Genesis", "1:1", "In the beginning God created ..."), ...)
    }

    val verses = readTextFileToVector(inputTextFileURL)

    def sampleVerse(verses: Vector[(String, String, String)]) = verses(Rng.nextInt(verses.length))

    spark.range(numVerses)
      .map(_ => sampleVerse(verses))
      .toDF(colNames = "Book", "VerseNumber", "VerseText")
      .write.mode("overwrite").format("csv").option("header", "true").save(outputDir)

    spark.stop()
  }
}
