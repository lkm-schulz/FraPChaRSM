import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.emrserverless.AWSEMRServerless
import s3.S3Utils

object Sparkbench {

  val DB_SCALE_FACTOR = 1000
  val DB_NAME = s"dataset_tpcds_${DB_SCALE_FACTOR}G"

  def main(args: Array[String]): Unit = {

    val mode = args(0)
    val storagePath = args(1)

    // val spark = SparkSession.builder.appName("Data Generator").config("hive.metastore.uris", "http://fs0:9083").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.appName("Data Generator").enableHiveSupport().getOrCreate()
    val s3 = S3Utils.getClientFromSparkConf(spark.sparkContext.getConf)
    //
    //    QueryStats.collectAll(spark, s3)
//    spark.stop()
//    return


    mode match {
      case "test" => TestWrite.run(storagePath, spark)
      case "datagen" => Datagen.data(storagePath, args(2), spark)
      case "metagen" => Datagen.metadata(storagePath, spark)
      case "query" => Query.run(args(2), spark)
//      case "cquery" => Query.runCustom(args(2), args(3), spark)
      case "tquery" => Query.runTest(storagePath, args(2), args(3).toInt, spark)
      case "queries_random" => Query.runRandomSelection(args(2).toInt, spark)
      case "workload" =>
        spark.sql(s"use database ${Sparkbench.DB_NAME}")
        val app = args(2)
        val startTime = if (args.length > 3) {
          println(s"Custom start time given: ${args(3)}")
          args(3).toLong * 1000
        } else System.currentTimeMillis
        val workload = workloads.Workload.fromFile(args(1))
        workload.run(spark, s3, app, startTime)

      case _ => throw new IllegalArgumentException("Unknown mode: " + mode)
    }

    spark.stop
  }

}
