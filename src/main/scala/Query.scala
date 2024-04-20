import org.apache.spark.sql.SparkSession
import com.databricks.spark.sql.perf.tpcds.TPCDS

object Query {
  def run(queryName: String, spark: SparkSession): Unit = {
    val sqlContext = spark.sqlContext

    spark.sql(s"use database ${ParquetGenerator.DB_NAME}")

    val tpcds = new TPCDS (sqlContext = sqlContext)

    val queryToRun = tpcds.tpcds2_4Queries.filter(q => q.name == queryName)

    tpcds.run(queryToRun)
  }
}
