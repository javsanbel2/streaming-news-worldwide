#!/bin/bash

echo "Testing consumer"

lines=(`cat "../config.txt"`)


${lines[1]}/bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --messages 100000 --topic test --threads 1 2
