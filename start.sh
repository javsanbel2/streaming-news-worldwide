#!/bin/bash

echo "We strongly recommend to run the tests first"
echo "Running servers"
./initialize.sh

echo "Running server spring collector"
cd collector
gnome-terminal --tab -- ./mvnw spring-boot:run
cd ..

echo "Running server scala consumer"
cd consumer
gnome-terminal --tab -- sbt run
cd ..

echo "Project running in two tabs... OK"


