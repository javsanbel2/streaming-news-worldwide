#!/bin/bash

lines=(`cat "servers.txt"`)
route=${lines[1]}

if [ $1 == "help" ]; then
	echo "Commands available"
	echo "list, create, listen"
fi
if [ $1 == "list" ]; then
	echo "Listing topics. No params"
	$route/bin/kafka-topics.sh --list --zookeeper localhost:2181
fi

if [ $1 == "create" ]; then
	echo "Creating topic. Param1*(topic)"
	$route/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $2
fi

if [ $1 == "listen" ]; then
	echo "Listening one topic. Param1*(topic)"
	$route/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $2 --from-beginning
fi
