#!/bin/bash

lines=(`cat "config.txt"`)

echo "Running Zookeeper & Hbase"
${lines[3]}/bin/start-hbase.sh

sleep 2

echo "Running Kafka"
gnome-terminal --tab -- ${lines[1]}/bin/kafka-server-start.sh ${lines[1]}/config/server.properties

echo "Implementing properties in projects"
appProperties="./collector/src/main/resources/application.properties"

sed -i '6,8d' $appProperties
echo "api.query="${lines[7]} >> $appProperties

echo "Creating topic in kafka"
./scripts.sh create newsapi

echo "Creating table in HBase"
echo "create '${lines[11]}','cf'" | ${lines[3]}/bin/hbase shell -n
status_code=$?
if [ ${status_code} -ne 0 ]; then
  echo "The command may have failed."
fi

