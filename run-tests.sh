#!/bin/bash

echo "Running servers"
./run-servers.bat

echo "Creating topic in kafka"
./kafka.bat create news

echo "Creating table in HBase"
lines=(`cat "servers.txt"`)

echo "create 'news','cf'" | ${lines[3]}/bin/hbase shell -n
status_code=$?
if [ ${status_code} -ne 0 ]; then
  echo "The command may have failed."
fi

echo "Running tests in collector project"
cd collector
./mvnw test
cd ..

echo "Running test in the consumer project"
cd consumer
./sbt test
cd ..

echo "All tests run properly!"
