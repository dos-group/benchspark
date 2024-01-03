package berlin.tu.dos.benchspark

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ThreadLocalRandom.{current => Rng}
import scala.math.max


object SelectWhereOrderByDataGeneration {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: SelectWhereOrderByDataGen <outputDir> <numUsers>");
      System.exit(1)
    }

    val outputDir = args(0)
    val numUsers = args(1).toLong

    val spark = SparkSession.builder.appName("SelectWhereOrderByDataGeneration").getOrCreate()
    import spark.implicits._


    val alphaNumericChars = ((48 to 57)++(97 to 122)++(65 to 90)).map(x => x.toChar)

    def randomSelection(seq: Seq[Char]) = seq(Rng.nextInt(seq.length))

    def randomString(minLength: Int, maxLength: Int): String = {
      (1 to Rng.nextInt(minLength, maxLength+1))
        .map(_ => randomSelection(alphaNumericChars)).mkString
    }

    def randomUserAge(): Int = max(0, 42 + (Rng.nextGaussian()*20).toInt)

    def generateUser(id: Long) = (id, randomString(5, 15), randomUserAge())

    spark.range(numUsers)
      .map(id => generateUser(id))
      .toDF(colNames= "id", "userName", "userAge")
      .write.mode("overwrite").format("csv").option("header", "true").save(outputDir)

    spark.stop()
  }
}
