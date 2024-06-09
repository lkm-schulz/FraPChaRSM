package workloads

import org.apache.spark.sql.SparkSession
// TODO: maybe i need to change this to more executors because otherwise the driver blocks new tasks..
import scala.concurrent.ExecutionContext.Implicits.global
import queries.QueryUtils
import scala.concurrent.Future

case class QuerySubmission(app: String, time: Long, query: String, range: String) {

  private val queryText = QueryUtils.getQueryWithDate(query, range)

  def runWhenReady(spark: SparkSession, startTime: Long): Future[Long] = {
    // block while its not time yet:
    println(s"Submission for query '$query' at T+$time ms waiting for its time to shine...")
    while (System.currentTimeMillis() - startTime < time) Thread.sleep(100)
    // then spawn the task but already return the future:
    println(s"Submitting '$query' at T+$time ms (${System.currentTimeMillis - startTime - time} ms delay)...")
    Future(run(spark, System.nanoTime))
  }

  def run(spark: SparkSession, runtimeStart: Long): Long = {
    // print submission time to check for issues with thread pool:
    println(s"Query '$query' at T+$time ms submitted (${(System.nanoTime - runtimeStart) / 1000000} ms after submission was started).")
    spark.sql(queryText).count
    val runtimeTotal = (System.nanoTime - runtimeStart) / 1000000
    println(s"Submission for query '$query' at T+$time ms finished in $runtimeTotal ms!")
    runtimeTotal
  }
}