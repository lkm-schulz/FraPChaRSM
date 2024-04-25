#!/bin/bash

mkdir .tmp
cd .tmp
wget https://download.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-amd64.tar.gz
tar xvzf ./influxdb2-client-2.7.5-linux-amd64.tar.gz
sudo mv ./influx /usr/local/bin/
cd ..
rm -r .tmp

influx config create --config-name sparkbench --host-url http://localhost:8086 --token token --org sparkbench --active

