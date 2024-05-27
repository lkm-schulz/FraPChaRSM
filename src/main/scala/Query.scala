import org.apache.spark.sql.SparkSession
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.functions.rand
import scala.io.Source

object Query {

  private def timeNRuns(code: => Unit, n: Int = 2, info: String = ""): Array[Long] = {
    (0 until n).map(run => {
      val startTime = System.nanoTime
      code
      (System.nanoTime - startTime) / 1000000
    }).toArray
  }

  def run(queryName: String, spark: SparkSession): Unit = {
    val tpcds = getTPCDS(spark)

    val queryToRun = tpcds.tpcds2_4Queries.filter(q => q.name == queryName)
    if (queryToRun.isEmpty) {
      throw new IllegalArgumentException(s"Unknown query name: $queryName\nAvailable queries: ${tpcds.tpcds2_4Queries.map(_.name).mkString(", ")}")
    }

    tpcds.run(queryToRun)
  }

  def runRandomSelection(count: Int, spark: SparkSession): Unit = {
    val tpcds = getTPCDS(spark)

    val queries = tpcds.tpcds2_4Queries
    for (i <- 1 to count) {
      val i = (math.random * queries.length).floor.toInt
      val queryToRun = Seq(queries(i))
      tpcds.run(queryToRun)
    }
  }

  def runTest(storagePath: String, queryName: String, numRows: Int, spark: SparkSession): Unit = {

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

  def getTime(spark: SparkSession, query: String, dateRange: String, numRuns: Int = 1, aggregator: String = "best"): Long = {
    spark.sql(s"use database ${ParquetGenerator.DB_NAME}")
    val filename = s"/opt/sparkbench/queries/$query.sql"
    val bufferedSource = Source.fromFile(filename)
    val queryText = bufferedSource.getLines.mkString("\n").replace("$DATERANGE$", dateRange)
    bufferedSource.close()

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

  private def getTPCDS(spark: SparkSession): TPCDS = {
    spark.sql(s"use database ${ParquetGenerator.DB_NAME}")
    new TPCDS(sqlContext = spark.sqlContext)
  }
}
