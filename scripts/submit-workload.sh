#!/bin/bash

# influx_query_preamble="from(bucket:\"telegraf\") |> range(start: $((script_start_time - COLLECTION_TIME_PRE_START)))"
# influx_query_cpu="$influx_query_preamble |> filter(fn: (r) => r._measurement == \"cpu\")"
# influx_query_mem="$influx_query_preamble |> filter(fn: (r) => r._measurement == \"mem\" and (r._field == \"used\" or r._field == \"available\" or r._field == \"total\" or r._field == \"free\" or r._field == \"shared\" or r._field == \"buffered\"))"

# echo "$influx_query_mem"
# exit 0

source scripts/misc/icons.sh

DIR_DATA=./data
DIR_TMP_DATA=$DIR_DATA/.tmp
DIR_TELEGRAF=$DIR_DATA/telegraf
DIR_LOGS=$DIR_DATA/driver-logs
DIR_DYNALLOC=$DIR_DATA/driver-logs
DIR_TRACES=$DIR_DATA/workload-traces

DIR_POD_TEMPLATES=./pod-templates
PATH_EXECUTOR_TEMPLATE=$DIR_POD_TEMPLATES/executor.yaml
DIR_POD_TEMPLATES_TMP=$DIR_POD_TEMPLATES/.tmp

S3_BUCKET_TRACES=sparkbench/data/workload-traces
S3_BUCKET_DYNALLOC=sparkbench/data/dynalloc-logs

COLLECTION_TIME_POST_END=20
COLLECTION_TIME_PRE_START=10

SPARK_SUBMIT="./work-dir/spark-3.5.1-bin-hadoop3/bin/spark-submit"
SPARK_FLAGS="\
--class Sparkbench --master k8s://https://192.168.185.2:6443 \
--conf spark.kubernetes.driver.podTemplateFile=./pod-templates/driver.yaml \
--deploy-mode cluster\
"
SPARK_JAR="local:///opt/sparkbench/sparkbench_2.12-1.0.jar"
SPARK_MODE="workload"

# Check if an argument is provided
if [ "$#" -lt 3 ]; then
    echo "$ICN_ERR Wrong number of arguments. Expected at least 3, given: $#"
    exit 1
fi

# Check if SPARK_SUBMIT file exists
if [ ! -f "$SPARK_SUBMIT" ]; then
    echo "$ICN_ERR '$SPARK_SUBMIT' not found. Are you in the root directory of the project?"
    exit 1
fi

num_apps=$1
workload_file=$2
start_delay=$3
add_conf=$4

echo "$ICN_INF submit-workload.sh (num_apps=$num_apps, workload='$workload_file', start_delay=$start_delay, add_conf='$add_conf')"

echo "$ICN_WAIT Creating pod templates..."
mkdir -p $DIR_POD_TEMPLATES_TMP
for ((i=0; i<$num_apps; i++)); do
    sed "s/\$(SPARK_APP_ID)/$i/g" $PATH_EXECUTOR_TEMPLATE > $DIR_POD_TEMPLATES_TMP/executor_$i.yaml
done

# Record start time
script_start_time=$(date +%s)
start_time=$((script_start_time + start_delay))
echo "$ICN_INF Script start time: $script_start_time, Workload start time: $start_time"

session_id=$workload_file/$start_time
echo "$ICN_INF SessionID: '$session_id'"

mkdir -p $DIR_TMP_DATA

# Submit spark apps:
for ((i=0; i<$num_apps; i++)); do
    echo "$ICN_WAIT Starting app $((i + 1))/$1..."

    conf_executor_template="--conf spark.kubernetes.executor.podTemplateFile=$DIR_POD_TEMPLATES_TMP/executor_$i.yaml"

    command="$SPARK_SUBMIT $SPARK_FLAGS $conf_executor_template $add_conf $SPARK_JAR $SPARK_MODE $workload_file $i $start_time"
    echo "$command"
    ( $command ) 2> >( tee $DIR_TMP_DATA/${start_time}_$i.txt | grep -v "INFO LoggingPodStatusWatcherImpl: Application status" ) &
done

echo "$ICN_SUCCESS All apps started!"

# Wait for all subshells to finish
wait

# Record end time (not really needed because the metrics can't be from the future anyways)
end_time=$(date +%s)
echo "$ICN_INF Execution took $((end_time - start_time)) seconds since the given start time (+ $((start_time - script_start_time)) seconds of setup wait)"

# Make dir for the session logs
dir_logs_session=$DIR_LOGS/$workload_file/$start_time
mkdir -p $dir_logs_session

# get the driver pod names
pod_names=$(grep --no-filename -oP 'pod name: \K.*' $DIR_TMP_DATA/${start_time}_*.txt | sort | uniq)

# save driver pod logs
for pod in $pod_names; do
    if kubectl logs $pod > $dir_logs_session/$pod.txt; then
        echo "$ICN_SUCCESS Logs of '$pod' saved to '$dir_logs_session/$pod.txt'!"
    fi
done

rm $DIR_TMP_DATA/${start_time}_*.txt

s3_cp_if_exists()
{
	bucket=$1
    dst=$2
    name=$3

    if ./bin/mc find $bucket > /dev/null 2>&1; then
        ./bin/mc cp -r $bucket $dst
        echo "$ICN_SUCCESS $name saved to $dst!"
    else
        echo "$ICN_WARN Bucket $bucket does not exist in the S3 storage. Something must have gone wrong with the collection in the applications."
    fi
}


s3_bucket_traces_session=$S3_BUCKET_TRACES/$session_id

s3_cp_if_exists $s3_bucket_traces_session/ $DIR_TRACES/$session_id "Workload traces"

s3_bucket_dynalloc_session=$S3_BUCKET_DYNALLOC/$session_id
s3_cp_if_exists $s3_bucket_dynalloc_session/ $DIR_DYNALLOC/$session_id "DynAlloc logs"

# if ./bin/mc find $s3_bucket_traces_session > /dev/null 2>&1; then
#     ./bin/mc cp -r $s3_bucket_traces_session $DIR_TRACES/$workload_file/
#     echo "$ICN_SUCCESS Workload traces saved to $DIR_TRACES/$session_id"
# else
#     echo "$ICN_WARN Bucket $s3_bucket_traces_session does not exist in the S3 storage. Something must have gone wrong with the trace collection in the applications."
# fi

echo "$ICN_WAIT Sleeping for $COLLECTION_TIME_POST_END seconds before collecting the metrics..."

sleep $COLLECTION_TIME_POST_END

dir_telegraf_session=$DIR_TELEGRAF/$session_id
mkdir -p $dir_telegraf_session

influx_query_preamble="from(bucket:\"telegraf\") |> range(start: $((script_start_time - COLLECTION_TIME_PRE_START)))"
influx_query_cpu="$influx_query_preamble |> filter(fn: (r) => r._measurement == \"cpu\")"
influx_query_mem="$influx_query_preamble |> filter(fn: (r) => r._measurement == \"mem\" and (r._field == \"used\" or r._field == \"available\" or r._field == \"total\" or r._field == \"free\" or r._field == \"shared\" or r._field == \"buffered\"))"

if influx query "$influx_query_cpu" --raw >> $dir_telegraf_session/cpu.csv; then
    echo "$ICN_SUCCESS Saved CPU metrics with query '$influx_query_cpu' to $dir_telegraf_session/cpu.csv"
fi

if influx query "$influx_query_mem" --raw >> $dir_telegraf_session/mem.csv; then
    echo "$ICN_SUCCESS Saved memory metrics with query '$influx_query_cpu' to $dir_telegraf_session/mem.csv"
fi

echo "$ICN_SUCCESS Workload run finished! SessionID: '$session_id'"