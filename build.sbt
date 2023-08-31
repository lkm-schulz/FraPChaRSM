name := "Parquet Data Generator"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "com.databricks" % "spark-sql-perf_2.12" % "0.5.1" from "file:///home/stalluri/picklejars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar"
