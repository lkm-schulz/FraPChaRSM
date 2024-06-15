#!/bin/bash

source ./scripts/misc/icons.sh

num_nodes=$1
num_drivers=$2

echo "$ICN_WAIT Untaiting and unlabeling all nodes. Ignore the errors..."

nodes=""
for ((i=0; i<$num_nodes; i++)); do
    nodes+="cloud${i}lennart "
done

kubectl taint nodes $nodes role=driver:NoSchedule-
kubectl label nodes $nodes driver-node-

echo "$ICN_WAIT Taiting and labeling driver nodes..."

driver_nodes=""
for ((i=0; i<$num_drivers; i++)); do
    driver_nodes+="cloud${i}lennart "
done

kubectl taint nodes $driver_nodes role=driver:NoSchedule
kubectl label nodes $driver_nodes driver-node=true