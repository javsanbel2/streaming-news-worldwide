# Streaming news world wide


We will get a big amount of streaming data from the NewsAPI. We will store this requested data into a Kafka cluster through a Java spring boot application. After we will process this data with Spark and store in HBase and Hive.

![Pipeline](https://i.ibb.co/V9SG7VG/General-pipeline-2.png)

## Structure

> + collector: Get information from APIs and introduce this data into Kafka Cluster  
> + consumer: Receive information and process via Spark streaming and save it into Hive & HBase  
> - start.sh : Script to start project  
> - test.sh: Script to run tests in both projects  
> - scripts.sh: Scripts to manage kafka and stop servers  
> - config.txt: Configuration to apply in the project (query to run, time intervals...)  
  
## Run project

Get into the folder and:

./test.sh
./run.sh


Documentation v1 (Updated) : [Google slides](https://docs.google.com/presentation/d/1AnSjBWKitK9Y74C_RbV58FL3HV-u5VgIj4SbMPWP84o/edit?usp=sharing)
Documentation v2 : [Google doc](https://docs.google.com/document/d/1nfjselzvqzASptw_VASdLWIO7K77BOrIVmp49HHik2g/edit?usp=sharing)
