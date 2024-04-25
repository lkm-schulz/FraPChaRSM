#!/bin/bash

# Not needed anymore

ORG=sparkbench
TOKEN=token

influx bucket $1 -n cloud_controller_${USER} -o $ORG -t $TOKEN
for ((i=0;i<=$2;i++)); do
    influx bucket $1 -n cloud${i}_${USER} -o $ORG -t $TOKEN
done
