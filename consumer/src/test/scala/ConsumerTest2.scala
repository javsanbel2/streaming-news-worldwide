import java.nio.file.{Files, Paths}
import java.util.zip.CRC32

import ch.qos.logback.classic.{Level, Logger}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.scalatest._
import org.slf4j.LoggerFactory

import scala.io.Source

class ConsumerTest2 extends FunSuite with BeforeAndAfterAll {

  System.setSecurityManager(null)
  private var stream:  DStream[ConsumerRecord[String, String]] = _
  private var ssc: StreamingContext = _
  val topics = Array("testOrder", "testCrc")


  override def beforeAll(): Unit = {
    // Configuring environment
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[*]")
      .set("hive.metastore.warehouse.dir", "spark-warehouse")
      .set("spark.sql.catalogImplementation","hive")
      .setAppName("StreamingNewsTest")
    ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    // Starting stream
    val kafkaParams = Utils.createKafkaParams()
    stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    // Wait to connect file stream
    Thread.sleep(3000)
  }

  test("Checking context") {
    assert(1 === 1)
    info("Context works")
  }

  test("Testing all methods of project") {

    // 1 ======== Getting log information
//    val crcText = Source.fromFile("../tests_output/logCrc.txt")
//    val checkCrc = crcText.getLines().drop(1).next()
//    crcText.close()
//
//    val orderText = Source.fromFile("../tests_output/logOrder.txt")
//    val checkOrder = crcText.getLines().drop(1).next()
//    orderText.close()

    // 2 ========= Parsing information
    val records = stream
      .filter(r => !r.value().contentEquals(""))
      .map(r => (r.topic(), r.value()))
    records.print()


    val checkCrc = 4157704578L
    val testCrc = records
      .filter(r => r._1.contentEquals("testCrc"))
      .map(r => r._2)
    testCrc.print()
    testCrc.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val res = rdd.collect()(0)
        val crc = new CRC32
        crc.update(res.getBytes())
        assert(crc.getValue.equals(checkCrc))
      }
    })

    val checkOrder = Array("1", "2", "3", "4", "5", "6", "7", "8", "9")
    val testOrder = records
      .filter(r => r._1.contentEquals("testOrder"))
      .map(r => r._2)
    testOrder.print()
    testOrder.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val arr = rdd.collect()
        assert(checkOrder.deep == arr.deep)
      }
    })


    ssc.start()
    ssc.awaitTerminationOrTimeout(100*60)
  }

}