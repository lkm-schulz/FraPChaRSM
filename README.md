# spark-data-generator
Generates dummy parquet, csv, json files for testing and validating MinIO S3 API compatibility.

The program support 4 modes: test, datagen, metagen, query

### Usage
Assuming you have java 11 and sbt installed. Use sdkman to install them if you don't.

Make sure `conf/spark-defaults.conf` in your spark installation diorectory are configured properly.

```
~ spark-submit --class "ParquetGenerator" --master spark://masternode:7077 \
    --driver-memory 8G --executor-memory 200G --total-executor-cores 256 \
    target/scala-2.12/parquet-data-generator_2.12-1.0.jar [mode] [storagePath] [dsdgenPath] [queryName]
~ mode validOptions ...
~ test storagePath
~ datagen storagePath dsdgenPath
~ metagen storagePath
~ query queryName
```

For `metagen` and `query`, the appropriate hive metastore should be configured in spark-defaults.conf.

For `datagen` and `metagen`, the appropriate GCS keyfile needs to be mounted using docker or kubernetes.

The project provides a Dockerfile which can be used to build a docker image with the necessary libraries and binaries using `docker build -t sacheendra/contispark:something -f docker/Dockerfile .`.
