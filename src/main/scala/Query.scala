import com.amazonaws.services.s3.model.ObjectMetadata
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.SparkSession
import utilities.S3

import java.io.ByteArrayInputStream
import scala.io.Source

class Query(spark: SparkSession) extends Sparkbench(spark) {

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

  def getTimeAndCount(queryName: String, dateRange: String, numRuns: Int = 1): Array[(Long, Long)] = {
    val queryText = Query.getQueryWithDate(queryName, dateRange)

    (0 until numRuns).map(run => {
      val startTime = System.nanoTime
      val count = spark.sql(queryText).count
      val time = (System.nanoTime - startTime) / 1000000
      (time, count)
    }).toArray
  }

  def getTime(queryName: String, dateRange: String, numRuns: Int = 1): Array[Long] = {

    val queryText = Query.getQueryWithDate(queryName, dateRange)

    Query.timeNRuns({
      spark.sql(queryText).show(1)
    }, numRuns)
  }

  def collectStats(overwriteExisting: Boolean = false): Unit = {
    // run a single query to get the overhead out of the way:
    getTime("q1", "d_year = 2000 and d_moy = 1")

    // get all available queries
    val queries = utilities.Files.list(Query.PATH_DIR_QUERIES).filter(_.endsWith(".sql")).map(_.stripSuffix(".sql"))
      .sortBy(_.stripPrefix("q").toInt)
    println("Queries found: '" + queries.mkString("', '") + "'")

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

      val results = collection.mutable.Map() ++ Query.resultsMapFromCSV(content)
      val ranges: Array[String] = try{
        dateRanges(query).arr.map(_.str).toArray
      }
      catch {
        case e: NoSuchElementException =>
          println(s"No custom date ranges found for query '$query' - using default.")
          rangesDefault
      }

      for (range <- ranges) {
        if (results.contains(range)) {
          println(s"Skipping range '$range' for query '$query', results already exist.")
        }
        else {
          println(s"Starting query '$query' with range '$range'...")
          val result = getTimeAndCount(query, range)(0)
          println(s"Returned ${result._2} rows in ${result._1} ms.")
          results(range) = result
        }
      }

      // write results back to storage
      val resultsCSVBytes = (results.map(res => s"${res._1},${res._2._1},${res._2._2}").mkString("\n") + '\n').getBytes("UTF-8")
      val metadata = new ObjectMetadata()
      metadata.setContentLength(resultsCSVBytes.length)
      s3.putObject(Query.S3_BUCKET_QUERIES, s"$query.csv", new ByteArrayInputStream(resultsCSVBytes), metadata)
    }
  }
}

object Query {

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

  def getQueryWithDate(name: String, dateRange: String = ""): String = {
    val filename = s"/opt/sparkbench/queries/$name.sql"
    val bufferedSource = Source.fromFile(filename)
    val queryText = bufferedSource.getLines.mkString("\n").replace("$DATERANGE$", dateRange)
    bufferedSource.close()
    queryText
  }

  private def resultsMapFromCSV(csv: String): Map[String, (Long, Long)] = {

    if (csv.isEmpty) {
      return Map.empty
    }

    try {
      csv.split("\n").map(line => {
        val cells = line.split(",")
        cells(0) -> (cells(1).strip().toLong, cells(2).strip.toLong)
      }).toMap
    }
    catch {
      case e: NumberFormatException => {
        println("Error while parsing number from CSV file: " + e.getMessage)
        println("Ignoring existing file.")
        Map.empty
      }
    }
  }

}
