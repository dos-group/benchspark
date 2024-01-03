package berlin.tu.dos.benchspark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.regression.{LinearRegression => LR}


object LinearRegression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

    if (args.length < 3){
      System.err.println("Usage: LinearRegression <inputFile:libsvm_format>" +
        " <MaxIterations> [<numberOfFeatures>]")
      System.exit(1)
    }
    Utils.logArgs("LinearRegression", args)

    val inputFile: String = args(0)
    val maxIterations: Int = args(1).toInt
    val numFeatures = if (args.length > 2) Option(args(2)) else None

    val elasticNetParameter: Double = 0.0  // 0.0 => L2 regularization (Ridge)
    val regularizationParameter: Double = 0.01
    val convergenceTolerance: Double = 1E-6

    val training: DataFrame = loadData(spark, inputFile, numFeatures)
    training.cache()

    val lr = new LR()
      .setRegParam(regularizationParameter)
      .setElasticNetParam(elasticNetParameter)
      .setMaxIter(maxIterations)
      .setTol(convergenceTolerance)

    val lrModel = lr.fit(training)

    println(s"Weights:\n${lrModel.coefficients}\nIntercept:\n${lrModel.intercept}")

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
