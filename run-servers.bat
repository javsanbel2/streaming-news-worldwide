#!/bin/bash

$filename = "servers.txt"

if [ $1 ] && [ $1 -eq 3 ]; then
	echo "Script to run Zookeeper, Kafka and application"
else
	echo "Script to run Zookeper and Kafka"
fi

echo "Starting..."
lines=(`cat "servers.txt"`)

echo "Running Zookeper"
gnome-terminal --tab -- ${lines[1]}/bin/zookeeper-server-start.sh ${lines[1]}/config/zookeeper.properties

echo "Running Kafka"
gnome-terminal --tab -- ${lines[3]}/bin/kafka-server-start.sh ${lines[3]}/config/server.properties

if [ $1 ]; then
	echo "Running server spring"
	./mvnw spring-boot:run
fi
