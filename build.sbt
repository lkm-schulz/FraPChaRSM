name := "Parquet Data Generator"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "com.databricks" % "spark-sql-perf_2.12" % "0.5.1" from "file:///var/scratch/stalluri/picklejars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar"
