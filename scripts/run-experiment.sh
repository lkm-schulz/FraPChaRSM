#!/bin/bash

source ./scripts/misc/icons.sh

mode=$1
num_apps=$2
workload=$3
initial_delay=$4

echo "$ICN_INF run_experiment.sh (mode='${mode}', num_apps=$num_apps, workload='$workload', initial_delay=$initial_delay)"

NUM_NODES=12
DIR_LOGS=./data/experiment-logs

mkdir -p $DIR_LOGS

./scripts/configure-nodes.sh $NUM_NODES $num_apps

conf=""

memory=""
executors=""
add_conf=""

case $mode in
    static)
        memory="24g"
        case $num_apps in
            2)
                executors=5
                ;;
            3)
                executors=3
                ;;
            4)
                executors=2
                ;;
        esac
        conf+="--conf spark.executor.instances=$executors "
        ;;

    dynamic)
        memory="24g"
        max_executors=""
        case $num_apps in
            2)
                max_executors=10
                ;;
            3)
                max_executors=9
                ;;
            4)
                max_executors=8
                ;;
        esac
        conf+="\
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dyanmicAllocation.minExecutors=0 \
--conf spark.dynamicAllocation.maxExecutors=$max_executors \
--conf spark.dynamicAllocation.initialExecutors=0 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.shuffleTracking.timeout=15s \
--conf spark.dynamicAllocation.shuffleTracking.enabled=true \
"
        ;;
        
    shared)
        cpu=""
        case $num_apps in
            2)
                memory="12g"
                executors=10
                cpu="1900m" # 4000 available, 100 for kube-flannel, some slack
                ;;
            3) 
                memory="8g"
                executors=9
                cpu="1200m"
                ;;
            4)
                memory="6g"
                executors=8
                cpu="900m"
                ;;
        esac
        conf+="--conf spark.kubernetes.executor.request.cores=$cpu --conf spark.executor.instances=$executors "
        ;;
    *)
        echo "Invalid mode: $mode"
        exit 1
        ;;
esac

conf+="--conf spark.executor.memory=$memory"

echo "$ICN_INF Configuration: '$conf'"

./scripts/submit-workload.sh $num_apps $workload $initial_delay "$conf"
