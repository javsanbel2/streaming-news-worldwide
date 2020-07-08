# Streaming news world wide


First project getting into data engineering. We will get a big amount of streaming data from the NewsAPI. We will store this requested data into a Kafka cluster through a Java spring boot application. After we will process this data with Spark and store in HBase and Hive.

## Structure

> + collector: Get information from APIs and introduce this data into Kafka Cluster  
+ consumer: Receive information and process via Spark streaming and save it into Hive & HBase  
- start.sh : Script to start project  
- test.sh: Script to run tests in both projects  
- scripts.sh: Scripts to manage kafka and stop servers  
- config.txt: Configuration to apply in the project (query to run, time intervals...)  
  
## Run project

Get into the folder and:

./test.sh
./run.sh


Documentation: [Google doc](https://docs.google.com/document/d/1nfjselzvqzASptw_VASdLWIO7K77BOrIVmp49HHik2g/edit?usp=sharing)
