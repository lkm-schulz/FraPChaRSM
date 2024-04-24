import org.apache.spark.sql.SparkSession

object ParquetGenerator {

  val DB_SCALE_FACTOR = 100
  val DB_NAME = s"dataset_tpcds_${DB_SCALE_FACTOR}G"

  def main(args: Array[String]): Unit = {

    val mode = args(0)
    val storagePath = args(1)

    // val spark = SparkSession.builder.appName("Data Generator").config("hive.metastore.uris", "http://fs0:9083").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.appName("Data Generator").enableHiveSupport().getOrCreate()

    mode match {
      case "test" => TestWrite.run(storagePath, spark)
      case "datagen" => Datagen.data(storagePath, args(2), spark)
      case "metagen" => Datagen.metadata(storagePath, spark)
      case "query" => Query.run(args(2), spark)
      case _ => throw new IllegalArgumentException("Unknown mode: " + mode)
    }
  }
}
