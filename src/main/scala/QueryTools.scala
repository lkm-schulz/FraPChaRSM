import com.amazonaws.services.s3.model.ObjectMetadata
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.SparkSession
import utilities.S3

import java.io.ByteArrayInputStream
import scala.io.Source

import data_structures.QueryStat
import utilities.Queries.getQueryWithDate

class QueryTools(spark: SparkSession) extends Sparkbench(spark) {

  spark.sql(s"use database $DB_NAME")

  def runTPCDS(queryName: String): Unit = {
    val tpcds = new TPCDS(sqlContext = spark.sqlContext)

    val queryToRun = tpcds.tpcds2_4Queries.filter(q => q.name == queryName)
    if (queryToRun.isEmpty) {
      throw new IllegalArgumentException(s"Unknown query name: $queryName\nAvailable queries: ${tpcds.tpcds2_4Queries.map(_.name).mkString(", ")}")
    }

    tpcds.run(queryToRun)
  }

  def runTPCDS(count: Int): Unit = {
    val tpcds = new TPCDS(sqlContext = spark.sqlContext)

    val queries = tpcds.tpcds2_4Queries
    for (i <- 1 to count) {
      val i = (math.random * queries.length).floor.toInt
      val queryToRun = Seq(queries(i))
      tpcds.run(queryToRun)
    }
  }

  def getTimeAndCount(queryName: String, dateRange: String, numRuns: Int = 1, verbose: Boolean = false): Array[(Long, Long)] = {
    val queryText = getQueryWithDate(queryName, dateRange)

    (0 until numRuns).map(run => {
      if (verbose) println(s"Starting run ${run + 1}/$numRuns...")
      val startTime = System.nanoTime
      val count = spark.sql(queryText).count
      val time = (System.nanoTime - startTime) / 1000000
      (time, count)
    }).toArray
  }

  def getTime(queryName: String, dateRange: String, numRuns: Int = 1): Array[Long] = {

    val queryText = getQueryWithDate(queryName, dateRange)

    timeNRuns({
      spark.sql(queryText).show(1)
    }, numRuns)
  }

  def collectStats(queryName: String, numRuns: Int = 1, overwriteExisting: Boolean = false): Unit = {
    // run a single query to get some(?) overhead out of the way:
    getTime("q1", "d_year = 2000 and d_moy = 1")

    // get all available queries
    var queries = utilities.Files.list(Query.PATH_DIR_QUERIES).filter(_.endsWith(".sql")).map(_.stripSuffix(".sql"))
      .sortBy(_.stripPrefix("q").toInt)

    if (queryName != "all") {
      queries = queries.filter(_ == queryName)
    }

    if (queries.isEmpty) {
      throw new IllegalArgumentException(s"Unknown query name: $queryName\nAvailable queries: '${queries.mkString("', '")}'")
    }

    // read the default and specific date ranges
    val datesSource = Source.fromFile(Query.PATH_FILE_DATES)
    val dateRanges = ujson.read(datesSource.getLines.mkString("\n"))
    val rangesDefault = dateRanges("default").arr.map(_.str).toArray
    datesSource.close()

    for (query <- queries) {
      val content = if (!overwriteExisting && s3.doesObjectExist(Query.S3_BUCKET_QUERIES, s"$query.csv")) {
        println(s"Retrieving existing stats for query '$query' from storage...")
        S3.getObjectAsString(s3, Query.S3_BUCKET_QUERIES, s"$query.csv")
      }
      else {
        ""
      }

      var results = Query.statsFromCSV(content)
      val ranges: Array[String] = try{
        dateRanges(query).arr.map(_.str).toArray
      }
      catch {
        case _: NoSuchElementException =>
          println(s"No custom date ranges found for query '$query' - using default.")
          rangesDefault
      }

      for (range <- ranges) {
          println(s"Starting query '$query' with range '$range'...")
          val result = getTimeAndCount(query, range, numRuns)
          println(s"rows: ${result(0)._2}, avg time: ${result.map(_._1).sum / result.length.toDouble} min time: ${result.map(_._1).min}, max time: ${result.map(_._1).max}")
          results = results ++ result.map(res => QueryStat(range, res._1, res._2))
      }

      // write results back to storage
      val resultsCSV = (QueryStat.CSVHeader +: results.map(_.toCSV)).mkString("\n") + "\n"
      val resultsCSVBytes = resultsCSV.getBytes("UTF-8")
      val metadata = new ObjectMetadata()
      metadata.setContentLength(resultsCSVBytes.length)
      s3.putObject(Query.S3_BUCKET_QUERIES, s"$query.csv", new ByteArrayInputStream(resultsCSVBytes), metadata)
    }
  }
}

object QueryTools {

  val PATH_DIR_QUERIES: String = "/opt/sparkbench/queries"
  val PATH_FILE_DATES: String = PATH_DIR_QUERIES + "/dates.json"
  val S3_BUCKET_QUERIES: String = "data/query-stats"

  private def timeNRuns(code: => Unit, n: Int = 2): Array[Long] = {
    (0 until n).map(run => {
      val startTime = System.nanoTime
      code
      (System.nanoTime - startTime) / 1000000
    }).toArray
  }

  private def statsFromCSV(csv: String): Array[QueryStat] = {

    if (csv.isEmpty) {
      return Array.empty
    }

    val header = csv.split("\n").head
    val rows = csv.split("\n").tail

    try {
      rows.map(row => QueryStat.fromCSV(row, header))
    }
    catch {
      case e: IllegalArgumentException =>
        println("Error while parsing CSV file: " + e.getMessage)
        println("Ignoring existing file.")
        Array.empty
    }
  }

}
