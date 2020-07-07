import java.io.File
import java.util.Date

import ch.qos.logback.classic.{Level, Logger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import play.api.libs.json._

object Consumer {
  def main(args: Array[String]): Unit = {
    // Term designated to the search key. It will be used to save in HBase
    val searchKey = "bitcoin"

    // Set logger to WARN
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.WARN)

    // Starting Spark instance
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingNews")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    val sparkSession = SparkSession.builder.appName("Saving in Hive").config(ssc.sparkContext.getConf).getOrCreate()

    // Starting stream
    val kafkaParams = createKafkaParams()
    val topics = Array("news")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    // Extract title and description for every request in NewsAPI
    val records = stream.flatMap(record => extractValues(record.value))
    records.print()
    val count = records.count()
    count.print()

    // Save information in HBase
    records.foreachRDD(rdd => saveToHBase(searchKey, rdd))

    // Save in Hive
    createTableHive(sparkSession, searchKey)
    records.foreachRDD(rdd => saveToHive(sparkSession, rdd, searchKey))

    ssc.start()
    ssc.awaitTermination()
  }

  def createTableHive(sparkSession: SparkSession, tableName: String): Unit = {
    sparkSession.sql(s"CREATE TABLE IF NOT EXISTS ${tableName}(title STRING, description STRING, publishedAt STRING)")
  }

  def saveToHive(sparkSession: SparkSession, rdd: RDD[(String, String, String)], tableName: String): Unit = {
    import sparkSession.implicits._

    val newsDataFrame = rdd.toDF()
    newsDataFrame.createOrReplaceTempView("newstest")

    try {
      newsDataFrame.write.mode("append").saveAsTable(tableName)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def saveToHBase(searchKey:String, rdd: RDD[(String, String, String)]): Unit = {
    val connection = ConnectionFactory.createConnection(createConfHBase())
    val table = connection.getTable(TableName.valueOf("news") )

    rdd.collect().foreach( row => {
      val rowKey = searchKey
      var put = new Put(Bytes.toBytes(row._3))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("title"), Bytes.toBytes(row._1))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("description"), Bytes.toBytes(row._2))

      table.put(put)
    })

    table.close()
    connection.close()
  }

  def createConfHBase(): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost:2181")
    conf.set("hbase.master", "localhost:60000")
    conf.set("hbase.rootdir", "file:///tmp/hbase")
    return conf
  }

  def extractValues(jsonString: String): Seq[(String, String, String)] = {
    // Parse string to json
    try {
      val json: JsValue = Json.parse(jsonString)
      // Get title and description
      val titles:Seq[String] = (json \\ "title").map(_.as[String])
      val descriptions:Seq[String] = (json \\ "description").map(_.as[String])
      val id:Seq[String] = (json \\ "publishedAt").map(_.as[String])
      // (title, description) tuples  -> return collection.immutable.$colon$colon
      val values = (titles, descriptions, id).zipped.toSeq

      return values
    } catch {
      case e: Exception => return Seq()
    }
  }

  def createKafkaParams(): Map[String, Object] = {
    return Map[String, Object](
      "client.dns.lookup" -> "resolve_canonical_bootstrap_servers_only",
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "news",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

}