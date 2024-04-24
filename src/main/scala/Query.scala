import org.apache.spark.sql.SparkSession
import com.databricks.spark.sql.perf.tpcds.TPCDS

object Query {
  def run(queryName: String, spark: SparkSession): Unit = {
    val tpcds = getTPCDS(spark)

    val queryToRun = tpcds.tpcds2_4Queries.filter(q => q.name == queryName)
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

  private def getTPCDS(spark: SparkSession): TPCDS = {
    spark.sql(s"use database ${ParquetGenerator.DB_NAME}")
    new TPCDS(sqlContext = spark.sqlContext)
  }
}
