package queries

import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.rand

import scala.io.Source

object Query {

  val PATH_DIR_QUERIES: String = "/opt/sparkbench/queries"
  val PATH_FILE_DATES: String = PATH_DIR_QUERIES + "/dates.json"
  val S3_BUCKET_QUERIES: String = "data/queries"

  private def timeNRuns(code: => Unit, n: Int = 2, info: String = ""): Array[Long] = {
    (0 until n).map(run => {
      val startTime = System.nanoTime
      code
      (System.nanoTime - startTime) / 1000000
    }).toArray
  }

  def runTPCDS(spark: SparkSession, dbName: String, queryName: String): Unit = {
    val tpcds = getTPCDS(spark, dbName)

    val queryToRun = tpcds.tpcds2_4Queries.filter(q => q.name == queryName)
    if (queryToRun.isEmpty) {
      throw new IllegalArgumentException(s"Unknown query name: $queryName\nAvailable queries: ${tpcds.tpcds2_4Queries.map(_.name).mkString(", ")}")
    }

    tpcds.run(queryToRun)
  }

  def runRandomSelection(spark: SparkSession, dbName: String, count: Int): Unit = {
    val tpcds = getTPCDS(spark, dbName)

    val queries = tpcds.tpcds2_4Queries
    for (i <- 1 to count) {
      val i = (math.random * queries.length).floor.toInt
      val queryToRun = Seq(queries(i))
      tpcds.run(queryToRun)
    }
  }

  def runTest(spark: SparkSession, storagePath: String, queryName: String, numRows: Int): Unit = {

    val location = s"${storagePath}/test"
    spark.sql(s"create database if not exists test location '${location}'")
    spark.sql("USE DATABASE test")

    val filename = s"/opt/sparkbench/queries/$queryName.sql"
    val bufferedSource = Source.fromFile(filename)
    val query = bufferedSource.getLines.mkString("\n")
    bufferedSource.close()

    timeNRuns({
      val df = spark.range(numRows)
      val df_random = df.select((rand() * numRows).alias("number"))
      df_random.write.mode("overwrite").option("path", location).saveAsTable("random_numbers")
    }, 2, "gen")

    timeNRuns(spark.sql(query).show(1), 2, "sort")
  }

  def getTime(spark: SparkSession, dbName: String, query: String, dateRange: String, numRuns: Int = 1, aggregator: String = "best"): Long = {
    spark.sql(s"use database $dbName")

    val queryText = QueryUtils.getQueryWithDate(query, dateRange)

    val times = timeNRuns({
      spark.sql(queryText).show(1)
    }, numRuns)

    aggregator match {
      case "best" => times.min
      case "worst" => times.max
      case "average" => times.sum / numRuns
      case "total" => times.sum
      case _ => throw new IllegalArgumentException(s"Unknown aggregator: $aggregator")
    }
  }

  private def getTPCDS(spark: SparkSession, dbName: String): TPCDS = {
    spark.sql(s"use database $dbName")
    new TPCDS(sqlContext = spark.sqlContext)
  }
}
