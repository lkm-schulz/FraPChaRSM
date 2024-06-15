import com.amazonaws.services.s3.AmazonS3
import org.apache.spark.sql.SparkSession
import utilities.S3

object Sparkbench {

  def main(args: Array[String]): Unit = {

    val mode = args(0)
    val spark = SparkSession.builder.appName("Data Generator").enableHiveSupport().getOrCreate()

    mode match {
      case "datagen" =>
        new Data(spark, storagePath = args(1))
          .generate(dsdgenPath = args(2))
      case "metagen" =>
        new Data(spark, storagePath = args(1))
          .structure()
      case "query_tpcds" =>
        new QueryTools(spark).runTPCDS(queryName = args(1))
      case "queries_tpcds" =>
        new QueryTools(spark).runTPCDS(count = args(1).toInt)
      case "query" =>
        val times = new QueryTools(spark).getTime(queryName = args(1), dateRange = args(2), numRuns = 2)
        println(s"Attempts took: ${times.mkString(" ms, ")} ms")
      case "query_stats" =>
        val queryName = args(1)
        val range = args(2)
        val numRuns = args(3).toInt
        args.length match {
          case 5 => new QueryTools(spark).collectStats(queryName, range, numRuns, bucket = args(4))
          case 4 => new QueryTools(spark).collectStats(queryName, range, numRuns)
          case _ => throw new IllegalArgumentException(s"Wrong number of arguments. Expected 4 or 5, got ${args.length}.")
        }
      case "workload" =>
        val startTime = if (args.length > 3) {
          println(s"Custom start time given: ${args(3)}")
          args(3).toLong * 1000
        } else System.currentTimeMillis / 1000 * 1000
        Workload.fromFile(spark, filename=args(1)).run(app = args(2), startTime)
      case _ =>
        throw new IllegalArgumentException("Unknown mode: " + mode)
    }
    println("main returning...")
  }
}

abstract class Sparkbench(val spark: SparkSession) {
  val DB_SCALE_FACTOR = 1000
  val DB_NAME = s"dataset_tpcds_${DB_SCALE_FACTOR}G"

  val s3: AmazonS3 = S3.getClientFromSparkConf(spark.sparkContext.getConf)
}