import com.amazonaws.services.s3.AmazonS3
import org.apache.spark.sql.SparkSession
import utilities.S3

object Sparkbench {

  def main(args: Array[String]): Unit = {

    val mode = args(0)
    val spark = SparkSession.builder.appName("Data Generator").enableHiveSupport().getOrCreate()

    mode match {
      case "update" =>
        val newLocation = args(1)
        spark.sql(s"ALTER DATABASE dataset_tpcds_1000G SET LOCATION '$newLocation/dataset_tpcds_1000G'")
        spark.sql(s"ALTER DATABASE dataset_tpcds_10G SET LOCATION '$newLocation/dataset_tpcds_10G'")
      case "datagen" =>
        new Data(spark, storagePath = args(1))
          .generate(dsdgenPath = args(2))
      case "metagen" =>
        new Data(spark, storagePath = args(1))
          .structure()
      case "query_tpcds" =>
        new Query(spark).runTPCDS(queryName = args(1))
      case "queries_tpcds" =>
        new Query(spark).runTPCDS(count = args(1).toInt)
      case "query" =>
        val times = new Query(spark).getTime(queryName = args(1), dateRange = args(2), numRuns = 2)
        println(s"Attempts took: ${times.mkString(" ms, ")} ms")
      case "query_stats" =>
        new Query(spark).collectStats()
      case "workload" =>
        val startTime = if (args.length > 3) {
          println(s"Custom start time given: ${args(3)}")
          args(3).toLong * 1000
        } else System.currentTimeMillis / 1000 * 1000
        Workload.fromFile(spark, filename=args(1)).run(app = args(2), startTime)
      case _ =>
        throw new IllegalArgumentException("Unknown mode: " + mode)
    }
  }
}

abstract class Sparkbench(val spark: SparkSession) {
  val DB_SCALE_FACTOR = 1000
  val DB_NAME = s"dataset_tpcds_${DB_SCALE_FACTOR}G"

  val s3: AmazonS3 = S3.getClientFromSparkConf(spark.sparkContext.getConf)
}