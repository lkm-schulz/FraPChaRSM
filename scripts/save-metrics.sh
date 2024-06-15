#!/bin/bash

influx_query_preamble="from(bucket:\"telegraf\") |> range(start: -${1}m)"
influx_query_cpu="$influx_query_preamble |> filter(fn: (r) => r._measurement == \"cpu\")"
influx_query_mem="$influx_query_preamble |> filter(fn: (r) => r._measurement == \"mem\" and (r._field == \"used\" or r._field == \"available\" or r._field == \"total\" or r._field == \"free\" or r._field == \"shared\" or r._field == \"buffered\"))"

DIR_METRICS=./data/telegraf/misc

start_time=$(date +%s)
start_time=$((start_time - ($1 * 60)))

dir_session=$DIR_METRICS/$start_time

mkdir -p $dir_session

if influx query "$influx_query_cpu" --raw >> $dir_session/cpu.csv; then
    echo "$ICN_SUCCESS Saved CPU metrics with query '$influx_query_cpu' to $dir_session/cpu.csv"
fi

if influx query "$influx_query_mem" --raw >> $dir_session/mem.csv; then
    echo "$ICN_SUCCESS Saved memory metrics with query '$influx_query_cpu' to $dir_session/mem.csv"
fi

echo "Session workload: misc"
echo "Session time: $start_time"
