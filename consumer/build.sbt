name := "consumer"

version := "0.1"

scalaVersion := "2.12.8"

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3" % "test"
// Logs
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"
// Hadoop & HBase
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.1"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.2.5"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.2.5"
// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.0"
// Hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.0.0"
// Parsing JSon
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.0"