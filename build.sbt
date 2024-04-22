name := "Parquet Data Generator"

version := "1.0"

scalaVersion := "2.12.18"

val currentDirectory = new java.io.File(".").getCanonicalPath

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-hadoop-cloud" % "3.5.1" from ("file://" + currentDirectory + "/docker/picklejars/spark-hadoop-cloud_2.12-3.5.1.jar")
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4" from ("file://" + currentDirectory + "/docker/picklejars/hadoop-aws-3.3.4.jar")
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262" from ("file://" + currentDirectory + "/docker/picklejars/aws-java-sdk-bundle-1.12.262.jar")
libraryDependencies += "com.databricks" % "spark-sql-perf_2.12" % "0.5.1" from ("file://" + currentDirectory + "/docker/picklejars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar")
