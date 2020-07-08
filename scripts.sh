#!/bin/bash

lines=(`cat "config.txt"`)
route=${lines[1]}

if [ $1 == "help" ]; then
	echo "Commands available"
	echo "list, create, listen, write, stop, collector"
fi
if [ $1 == "list" ]; then
	echo "Listing topics. No params"
	${lines[1]}/bin/kafka-topics.sh --list --zookeeper localhost:2181
fi

if [ $1 == "create" ]; then
	echo "Creating topic. Param1*(topic)"
	${lines[1]}/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $2
fi

if [ $1 == "listen" ]; then
	echo "Listening one topic. Param1*(topic)"
	${lines[1]}/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $2 --from-beginning
fi
if [ $1 == "write" ]; then
	echo "Writing in one topic. Param1*(topic)"
	${lines[1]}/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $2
fi
if [ $1 == "stop" ]; then
	echo "Stopping servers"
	${lines[1]}/bin/kafka-server-stop.sh
	${lines[3]}/bin/stop-hbase.sh
fi
if [ $1 == "collector" ]; then
        echo "Running server spring collector"
	cd collector
        eval "./mvnw spring-boot:run"
fi

