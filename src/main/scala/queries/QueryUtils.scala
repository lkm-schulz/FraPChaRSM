package queries

import scala.io.Source

object QueryUtils {
  def getQueryWithDate(name: String, dateRange: String = ""): String = {
    val filename = s"/opt/sparkbench/queries/$name.sql"
    val bufferedSource = Source.fromFile(filename)
    val queryText = bufferedSource.getLines.mkString("\n").replace("$DATERANGE$", dateRange)
    bufferedSource.close()
    queryText
  }
}
