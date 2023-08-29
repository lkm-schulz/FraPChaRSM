import com.databricks.spark.sql.perf.tpcds.{TPCDS, TPCDSTables}
import org.apache.spark.sql.SparkSession

object ParquetGenerator {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Data Generator").config("hive.metastore.uris", "http://fs0:9083").enableHiveSupport().getOrCreate()
    // val spark = SparkSession.builder.appName("Data Generator").getOrCreate()

    val sqlContext = spark.sqlContext

    for (i <- 0 to 10) {

      // Set:
      // Note: Here my env is using MapRFS, so I changed it to "hdfs:///tpcds".
      // Note: If you are using HDFS, the format should be like "hdfs://namenode:9000/tpcds"
      val rootDir = "hdfs://10.149.0.254:9000/tpcds_test" // root directory of location to create data in.
      val alluxioDir = "alluxio://10.149.0.254:19998"

      val databaseName = s"${i}dataset_tpcds" // name of database to create.
      val scaleFactor = if (i == 0) "1000" else "100" // scaleFactor defines the size of the dataset to generate (in GB).
      val format = "parquet" // valid spark format like parquet "parquet".
      // Run:
      val tables = new TPCDSTables(sqlContext,
        dsdgenDir = "/var/scratch/stalluri/tpcds-bin", // location of dsdgen
        scaleFactor = scaleFactor,
        useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
        useStringForDate = false) // true to replace DateType with StringType
      val location = s"${rootDir}/${databaseName}"
      val alluxioLoc = s"${alluxioDir}/${databaseName}"


      // Comment this out after generating data if you just want to repopulate the metastore
      tables.genData(
         location = location,
        format = format,
        overwrite = true, // overwrite the data that is already there
        partitionTables = true, // create the partitioned fact tables
        clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
        filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
        tableFilter = "", // "" means generate all tables
        numPartitions = scaleFactor.toInt) // how many dsdgen partitions to run - number of input tasks.

      // Create the specified database
      spark.sql(s"create database if not exists $databaseName location '${alluxioLoc}'")
      // Create metastore tables in a specified database for your data.
      // Once tables are created, the current database will be switched to the specified database.
      tables.createExternalTables(alluxioLoc, "parquet", databaseName, overwrite = true, discoverPartitions = true)
      // Or, if you want to create temporary tables
      // tables.createTemporaryTables(location, format)

      // For CBO only, gather statistics on all columns:
      tables.analyzeTables(databaseName, analyzeColumns = true)
    }
  }
}
