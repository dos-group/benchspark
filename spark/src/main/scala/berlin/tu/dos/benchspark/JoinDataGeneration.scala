package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ThreadLocalRandom.{current => Rng}


object JoinDataGeneration {

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: JoinDataGen <outputDir1> <outputDir2> <numPages> <numUserVisits>");
      System.exit(1)
    }

    val outputDir1 = args(0)
    val outputDir2 = args(1)
    val numPages = args(2).toLong
    val numUserVisits = args(3).toLong

    val spark = SparkSession.builder.appName("JoinDataGeneration").getOrCreate()
    import spark.implicits._

    val alphaNumericChars = ((48 to 57)++(97 to 122)).map(x => x.toChar)

    def randomSelection(seq: Seq[Char]) = seq(Rng.nextInt(seq.length))

    def randomString(minLength: Int, maxLength: Int): String = {
      (1 to Rng.nextInt(minLength, maxLength))
        .map(_ => randomSelection(alphaNumericChars)).mkString
    }

    def randomTimestamp() = Rng.nextInt(1532712002, 1690478472)  // 5 year span 2018-2023

    def randomIP() = (1 to 4).map(_ => Rng.nextInt(0, 255)).mkString(".")

    def randomBrowser() = Seq("Firefox", "Chromium", "Brave", "Netscape")(Rng.nextInt(4))

    def randomURL() = "https://"+randomString(3, 15)+Seq(".com", ".org", ".net")(Rng.nextInt(3))

    def randomPage() = Rng.nextLong(0,numPages)

    spark.range(numUserVisits)
    .map(_ =>
        (randomPage(), randomIP(), randomBrowser(), randomTimestamp())
    ).toDF(colNames = "pageId", "userIP", "usedBrowser", "timeStamp")
    .write.mode("overwrite").format("csv").option("header", "true").save(outputDir1)

    spark.range(numPages)
    .map(x =>
        (x, randomURL())
    ).toDF(colNames = "pageId", "pageURL")
    .write.mode("overwrite").format("csv").option("header", "true").save(outputDir2)

    spark.stop()
  }
}
