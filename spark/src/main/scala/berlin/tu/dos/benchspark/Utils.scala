package berlin.tu.dos.benchspark

import org.apache.spark.SparkContext


object Utils {

  def logArgs(app:String, args: Array[String]) : Unit = {
    val appParameters: String = args.mkString(", ")
    System.err.println(s"${app}(${appParameters})\n")
  }

  def logRuntime(sc: SparkContext) : Unit = {
    val startTimeMillis = sc.startTime
    val endTimeMillis = System.currentTimeMillis
    val durationInSeconds = (endTimeMillis-startTimeMillis)/1000.0

    System.err.println(f"start_time[unix_millis]:$startTimeMillis%d")
    System.err.println(f"end_time[unix_millis]:$endTimeMillis%d")
    System.err.println(f"net_runtime[seconds]:$durationInSeconds%.3f")
  }
}
