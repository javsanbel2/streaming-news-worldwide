#!/bin/bash

lines=(`cat "servers.txt"`)
route=${lines[1]}

if [ $1 == "list" ]; then
	echo "Listing topics"
	$route/bin/kafka-topics.sh --list --zookeeper localhost:2181
fi

if [ $1 == "create" ]; then
	echo "Creating topic"
	$route/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $2
fi
