# spark-data-generator
Generates dummy parquet, csv, json files for testing and validating MinIO S3 API compatibility.

The program support 4 modes: test, datagen, metagen, query

### Usage
Assuming you have java 11 and sbt installed. Use sdkman to install them if you don't.

Make sure `conf/spark-defaults.conf` in your spark installation directory are configured properly.

```
~ spark-submit --class "ParquetGenerator" \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \ \
    --deploy-mode cluster \
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

Upload the created image to a registry, then modify the container image to use in `docker/spark-conf/spark-defaults.conf`, and add the gcs keyfile as secret named `gcs-keyfile` to kubernetes.

Copy the `spark-defaults.conf` into your local spark conf directory, and then run the spark-submit command above.

For convenience, you could also use the docker image itself to run the spark-submit command.

Configuration options for spark performance here: https://spark.apache.org/docs/latest/running-on-kubernetes.html
