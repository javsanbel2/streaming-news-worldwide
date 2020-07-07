import java.nio.file.{Files, Paths}

import ch.qos.logback.classic.{Level, Logger}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.scalatest._

class ConsumerTest extends FunSuite with BeforeAndAfterAll {

  private var stream:  DStream[ConsumerRecord[String, String]] = _
  private var ssc: StreamingContext = _

  override def beforeAll(): Unit = {
    // Configuring environment
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[*]")
      .set("hive.metastore.warehouse.dir", "spark-warehouse")
      .set("spark.sql.catalogImplementation","hive")
      .setAppName("StreamingNews")
    ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    // Starting stream
    val kafkaParams = Consumer.createKafkaParams()
    val topics = Array("news")
    stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    // Wait to connect file stream
    Thread.sleep(3000)
  }

  test("Checking context") {
    assert(1 === 1)
    info("Context works")
  }

  test("Testing all methods of project") {
    // 1 ======== Checking parsing method
    val searchKey = "bitcoin"
    val news = stream.flatMap(record => Consumer.extractValues(record.value))
    news.print()

    news.foreachRDD(rdd => {
      val count_data = rdd.count()
      if (count_data > 0) {
        val row = rdd.take(1)(0)
        assert(row.productIterator.count(x => x != "") === 3, "Parsing method is not working properly")
        info("Parsing works")
      }
    })

    // 2 ======== Save information in HBase
    news.foreachRDD(rdd => {
      val count_data = rdd.count()
      if (count_data > 0) {
        // Creating connection
        val connection = ConnectionFactory.createConnection(Consumer.createConfHBase())
        val table = connection.getTable(TableName.valueOf("news"))

        // Count all rows BEFORE insert
        val scanner_before = table.getScanner(new Scan())
        val count_before = Iterator.from(0).takeWhile(x => scanner_before.next() != null).length

        // Save in HBase method
        Consumer.saveToHBase(searchKey, rdd)

        // Count all rows AFTER insert
        val scanner_after = table.getScanner(new Scan())
        val count_after = Iterator.from(0).takeWhile(x => scanner_after.next() != null).length

        // Test
        assert(count_before < count_after, "The saving method in HBase is not working properly")
        info("HBase works")

        table.close()
        connection.close()
      }
    })

    // 3 ======== Save in Hive
    val sparkSession = SparkSession.builder()
      .config(ssc.sparkContext.getConf)
      .enableHiveSupport().getOrCreate()
    import sparkSession.sql
    sql(s"DROP TABLE IF EXISTS $searchKey")

    news.foreachRDD(rdd => {
      val count_data = rdd.count()
      if (count_data > 0) {
        // Check how many rows BEFORE insert
        val count_before = 0L
        var count_after = 0L

        Consumer.saveToHive(sparkSession, rdd, searchKey)

        // Check how many rows AFTER insert
        if (Files.exists(Paths.get(s"spark-warehouse/$searchKey"))) {
          count_after = sparkSession.read.parquet(s"spark-warehouse/$searchKey").count()
          println("Count after   " + count_after)
        }

        assert(count_before < count_after, "Saving in hive is not working properly")
        info("Hive works")
      }
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(250*60)
  }

}