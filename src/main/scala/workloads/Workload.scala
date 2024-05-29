package workloads

import com.amazonaws.services.s3.AmazonS3
import org.apache.spark.sql.SparkSession

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.Source
import s3.S3Utils

class Workload(querySubmissions: List[QuerySubmission], name: String = "nA") {
  def run(spark: SparkSession, s3: AmazonS3, app: String, startTime: Long): Unit = {

    val appSubmissions = querySubmissions.filter(_.app == app)
    // note: this assumes the queries are sorted by time (it's blocking)
    val resultFutures = appSubmissions.map(_.runWhenReady(spark, startTime))

    val results = resultFutures.map(Await.result(_, Duration.Inf))
    val resultsBucket = s"${Workload.S3_BUCKET}/$name/$startTime"
    S3Utils.putStringAsObject(s3, resultsBucket, s"$app.csv", results.mkString("\n"))
  }
}

object Workload {

  private def DIR_WORKLOADS = "/opt/sparkbench/workloads/"
  private def CSV_HEADER = "app,start,query,range"
  private def S3_BUCKET = "data/workloads"

  def fromFile(filename: String): Workload = {
    val bufferedSource = Source.fromFile(DIR_WORKLOADS + filename + ".csv")
    val lines:List[String] = bufferedSource.getLines().toList.map(_.trim)
    bufferedSource.close()

    val headers = lines.head
    if (headers != CSV_HEADER) {
      throw new IllegalArgumentException(s"Invalid workload file format. Expected header: '$CSV_HEADER' found: '$headers")
    }

    val querySubmissions = lines.tail.map(line => {
      val fields = line.split(",").map(_.trim)
      QuerySubmission(fields(0), fields(1).toLong, fields(2), fields(3))
    })

    new Workload(querySubmissions, filename)
  }
}