package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession
import scala.io.Source
import java.util.concurrent.ThreadLocalRandom.{current => Rng}


object WordCountDataGeneration {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: WordCountDataGen <inputTextFileURL> <outputDir> <numWords>");
      System.exit(1)
    }

    // val inputTextFileURL = "https://openbible.com/textfiles/kjv.txt"  // Bible: King James Version
    val inputTextFileURL = args(0)
    val outputDir = args(1)
    val numWords = args(2).toLong

    val spark = SparkSession.builder.appName("WordCountDataGeneration").getOrCreate()
    import spark.implicits._

    def removeNonAlphabetic(word: String) = word.replaceAll("[^a-zA-Z]", "")

    def getWordsInFile(fileURL: String): Array[String] = {

      val lines: Array[String] = {
        if (inputTextFileURL.contains("http")) {
          Source.fromURL(fileURL).getLines().toArray[String]
        } else {
          spark.read.text(inputTextFileURL).map(_.getString(0)).collect()
        }
      }
      val words = lines
        .flatMap(_.split("\\s+"))
        .map(removeNonAlphabetic(_))
        .filter(_.nonEmpty)
        .map(_.toLowerCase)

      return words
    }

    def sampleWord(words: Array[String]) = words(Rng.nextInt(0, words.length))

    var words = getWordsInFile(inputTextFileURL)

    val text = spark.range(numWords)
    text.map(_ => sampleWord(words))
      .write.mode("overwrite").text(outputDir)

    spark.stop()
  }
}
