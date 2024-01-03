package berlin.tu.dos.benchspark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.classification.{LogisticRegression => LR}


object LogisticRegression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("LogisticRegression").getOrCreate()

    if (args.length < 3){
      System.err.println("Usage: LogisticRegression <inputFile:libsvm_format>" +
        " <MaxIterations> [<numberOfFeatures>]")
      System.exit(1)
    }
    Utils.logArgs("LogisticRegression", args)

    val inputFile: String = args(0)
    val maxIterations: Int = args(1).toInt
    val numFeatures = if (args.length > 2) Option(args(2)) else None

    val elasticNetParameter: Double = 0.0  // 0.0 => L2 regularization (Ridge)
    val regularizationParameter: Double = 0.01

    val training: DataFrame = loadData(spark, inputFile, numFeatures)

    training.cache()

    val lr = new LR()
      .setMaxIter(maxIterations)
      .setRegParam(regularizationParameter)
      .setElasticNetParam(elasticNetParameter)

    val lrModel = lr.fit(training)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    Utils.logRuntime(spark.sparkContext)
    spark.stop()
  }

  def loadData(spark: SparkSession, path: String,
                expectedNumFeatures: Option[String] = None): DataFrame = {

    expectedNumFeatures match {
        case Some(expectedNumFeatures) => spark.read
          .option("numFeatures", expectedNumFeatures.toString)
          .format("libsvm")
          .load(path)
        case None => spark.read.format("libsvm").load(path)
    }
  }
}
