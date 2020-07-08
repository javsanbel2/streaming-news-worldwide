#!/bin/bash

echo "Running servers"
./initialize.sh

echo "Running tests in collector project"
cd collector
./mvnw test
cd ..

echo "Running test in the consumer project"
cd consumer
sbt test
cd ..

echo "All tests run properly!"
