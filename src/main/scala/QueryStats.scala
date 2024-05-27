import com.amazonaws.services.s3.model.{GetObjectRequest, ObjectMetadata, PutObjectRequest}
import org.apache.spark.sql.SparkSession

import scala.io.Source
import java.io.ByteArrayInputStream
import com.amazonaws.services.s3.AmazonS3

object QueryStats {
  def collectAll(spark: SparkSession, s3: AmazonS3, overwriteExisting: Boolean = false): Unit = {
    // run a single query to get the overhead out of the way:
    Query.getTime(spark, "q1", "d_year = 2000 and d_moy = 1")

    // get all available queries
    val queries = listFiles(Settings.PATH_QUERIES).filter(_.endsWith(".sql")).map(_.stripSuffix(".sql"))
      .sortBy(_.stripPrefix("q").toInt)
    println("Queries found: '" + queries.mkString("', '") + "'")

    // read the default and specific date ranges
    val datesSource = Source.fromFile(Settings.PATH_FILE_DATES)
    val dateRanges = ujson.read(datesSource.getLines.mkString("\n"))
    val rangesDefault = dateRanges("default").arr.map(_.str).toArray
    datesSource.close()


    for (query <- queries) {
      val content = if (!overwriteExisting && s3.doesObjectExist(Settings.S3_BUCKET_QUERIES, s"$query.csv")) {
        println(s"Retrieving existing stats for query '$query' from storage...")
        S3Utils.getObjectAsString(s3, Settings.S3_BUCKET_QUERIES, s"$query.csv")
      }
      else {
        ""
      }

      val results = collection.mutable.Map() ++ resultsMapFromCSV(content)
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
          println(s"Starting query '$query' with range '$range'...'")
          val time = Query.getTime(spark, query, range)
          println(s"Took $time ms.")
          results(range) = time
        }
      }

      // write results back to storage
      val resultsCSVBytes = results.map(res => s"${res._1},${res._2}").mkString("\n").getBytes("UTF-8")
      val metadata = new ObjectMetadata()
      metadata.setContentLength(resultsCSVBytes.length)
      s3.putObject(new PutObjectRequest(Settings.S3_BUCKET_QUERIES, s"$query.csv", new ByteArrayInputStream(resultsCSVBytes), metadata))
    }
  }

  private def listFiles(path: String): List[String] = {
    val d = new java.io.File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.getName)
    } else {
      List[String]()
    }
  }

  private def resultsMapFromCSV(csv: String): Map[String, Long] = {

    if (csv.isEmpty) {
      return Map.empty
    }

    try {
      csv.split("\n").map(line => {
        val cells = line.split(",")
        cells(0) -> cells(1).strip().toLong
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
