import java.util.Date

import ch.qos.logback.classic.{Level, Logger}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import play.api.libs.json._

object Consumer {

  case class News(title: String, description: String)

  def main(args: Array[String]): Unit = {
    // Term designated to the search key. It will be used to save in HBase
    val searchKey = "bitcoin"
    // Set logger to WARN
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.WARN)
    // Starting Spark instance
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingNews")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    val kafkaParams = Map[String, Object](
      "client.dns.lookup" -> "resolve_canonical_bootstrap_servers_only",
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "news",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("news")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    // UNIT TESTING 1
    // Extract title and description for every request in NewsAPI
    val records = stream.flatMap(record => extractValues(record.value))
    records.print()
    val count = records.count()
    count.print()

    // UNIT TESTING 2

    // MANUAL TESTING

    // Save information in HBase
    records.foreachRDD ( rdd => {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "localhost:2181")
      conf.set("hbase.master", "localhost:60000")
      conf.set("hbase.rootdir", "file:///tmp/hbase")

      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf("news") )

      rdd.collect().foreach( row => {
        val rowKey = searchKey + "-" + new Date().getTime.toString()
        var put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("title"), Bytes.toBytes(row._1))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("description"), Bytes.toBytes(row._2))

        table.put(put)
      })

      table.close()
      connection.close()
    })

    // Hive table


    println("hello")

    ssc.start()
    ssc.awaitTermination()
  }

  def extractValues(jsonString: String): Seq[(String, String)] = {
    // Parse string to json
    val json: JsValue = Json.parse(jsonString)
    // Get title and description
    val titles:Seq[String] = (json \\ "title").map(_.as[String])
    val descriptions:Seq[String] = (json \\ "description").map(_.as[String])
    // (title, description) tuples  -> return collection.immutable.$colon$colon
    val values = titles.zip(descriptions)

    return values
  }

}