import org.apache.spark.sql.SparkSession

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.Source

import utilities.S3

class Workload(spark: SparkSession, querySubmissions: List[QuerySubmission], name: String = "nA") extends Sparkbench(spark) {

  def run(app: String, startTime: Long): Unit = {
    spark.sql(s"use database $DB_NAME")
    val appSubmissions = querySubmissions.filter(_.app == app)
    // note: this assumes the queries are sorted by time (it's blocking)
    val resultFutures = appSubmissions.map(_.runWhenReady(spark, startTime))

    val results = resultFutures.map(Await.result(_, Duration.Inf))
    val resultsBucket = s"${Workload.S3_BUCKET}/$name/${startTime / 1000}"
    val csv = (QuerySubmissionResult.CSVHeader :: results.map(_.toCSV)).mkString("\n") + "\n"
    S3.putStringAsObject(s3, resultsBucket, s"$app.csv", csv)
  }
}

object Workload {
  private val DIR_WORKLOADS = "/opt/sparkbench/workloads"
  private val S3_BUCKET = "data/workload-traces"

  private val HDR_APP = "app"
  private val HDR_START = "start"
  private val HDR_QUERY = "query"
  private val HDR_RANGE = "range"

  private val COLUMNS = Set(
    HDR_APP, HDR_START, HDR_QUERY, HDR_RANGE
  )

  def fromFile(spark: SparkSession, filename: String): Workload = {
    val bufferedSource = Source.fromFile(s"$DIR_WORKLOADS/$filename.csv")
    val lines:List[String] = bufferedSource.getLines().toList.map(_.trim)
    bufferedSource.close()

    val header = lines.head.split(",")
    if (!COLUMNS.forall(header.contains)) {
      throw new IllegalArgumentException(
        s"Invalid workload file. Expected headers: {${COLUMNS.mkString(", ")}} but missing: ${COLUMNS.filterNot(header.contains)}"
      )
    }

    val querySubmissions = lines.tail.map(line => {
      val fields = line.split(",").map(_.trim)
      QuerySubmission(
        app = fields(header.indexOf(HDR_APP)),
        start = fields(header.indexOf(HDR_START)).toLong,
        query = fields(header.indexOf(HDR_QUERY)),
        range = fields(header.indexOf(HDR_RANGE)))
    })

    new Workload(spark, querySubmissions, filename)
  }
}
