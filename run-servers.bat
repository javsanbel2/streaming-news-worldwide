#!/bin/bash

$filename = "servers.txt"

if [ $1 ] && [ $1 -eq 3 ]; then
	echo "Script to run Zookeeper, Hbase, Kafka and application"
else
	echo "Script to run Zookeper, Hbase and Kafka"
fi

echo "Starting..."
lines=(`cat "servers.txt"`)

echo "Running Zookeeper & Hbase"
gnome-terminal --tab -- ${lines[3]}/bin/start-hbase.sh

sleep 2

echo "Running Kafka"
gnome-terminal --tab -- ${lines[1]}/bin/kafka-server-start.sh ${lines[1]}/config/server.properties

if [ $1 ]; then
	echo "Running server spring"
	./mvnw spring-boot:run
fi
