#!/bin/bash

echo "Testing producer"

lines=(`cat "../config.txt"`)


${lines[1]}/bin/kafka-producer-perf-test.sh --topic test --num-records 100000 --throughput -1 --producer-props bootstrap.servers=localhost:9092 batch.size=18384 acks=all linger.ms=1 compression.type=none request.timeout.ms=300000 --record-size 17295
