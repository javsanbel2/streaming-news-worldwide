#!/bin/bash

$filename = "servers.txt"

echo "Script to run Zookeper, Hbase and Kafka"

echo "Starting..."
lines=(`cat "servers.txt"`)

echo "Running Zookeeper & Hbase"
${lines[3]}/bin/start-hbase.sh

sleep 2

echo "Running Kafka"
gnome-terminal --tab -- ${lines[1]}/bin/kafka-server-start.sh ${lines[1]}/config/server.properties

