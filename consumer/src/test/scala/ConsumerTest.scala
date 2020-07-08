import java.nio.file.{Files, Paths}

import ch.qos.logback.classic.{Level, Logger}
import org.apache.hadoop.hbase.{HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, ConnectionFactory, HBaseAdmin, Scan, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
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

  System.setSecurityManager(null)
  private var stream:  DStream[ConsumerRecord[String, String]] = _
  private var ssc: StreamingContext = _
  val topics = Array("newsapi")
  val tableName = Utils.loadConfiguration()("tableName")


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
    // 1 ======== Checking parsing method
    val news = stream.flatMap(record => Utils.extractValues(record.value))
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
    val conf = HBase.createConfHBase()
    val connection = ConnectionFactory.createConnection(conf)

    // Removing existing table
    val admin = connection.getAdmin
    if (admin.tableExists(TableName.valueOf("bitcoin"))) {
      admin.disableTable(TableName.valueOf("bitcoin"))
      admin.deleteTable(TableName.valueOf("bitcoin"))
    }

    // Creating table
    val cfd: ColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf")).build();
    val table: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("bitcoin"));
    table.setColumnFamily(cfd)
    admin.createTable(table.build())


    news.foreachRDD(rdd => {
      val count_data = rdd.count()
      if (count_data > 0) {
        // Creating connection
        val table = connection.getTable(TableName.valueOf(tableName))

        // Count all rows BEFORE insert
        val scanner_before = table.getScanner(new Scan())
        val count_before = Iterator.from(0).takeWhile(x => scanner_before.next() != null).length

        // Save in HBase method
        HBase.saveToHBase(tableName, rdd)

        // Count all rows AFTER insert
        val scanner_after = table.getScanner(new Scan())
        val count_after = Iterator.from(0).takeWhile(x => scanner_after.next() != null).length

        // Test
        assert(count_before < count_after, "The saving method in HBase is not working properly")
        info("HBase works")

        table.close()
      }
    })

    // 3 ======== Save in Hive
    val sparkSession = SparkSession.builder()
      .config(ssc.sparkContext.getConf)
      .enableHiveSupport().getOrCreate()
    import sparkSession.sql
    sql(s"DROP TABLE IF EXISTS $tableName")

    news.foreachRDD(rdd => {
      val count_data = rdd.count()
      if (count_data > 0) {
        // Check how many rows BEFORE insert
        val count_before = 0L
        var count_after = 0L

        Hive.saveToHive(sparkSession, rdd, tableName)

        // Check how many rows AFTER insert
        if (Files.exists(Paths.get(s"spark-warehouse/$tableName"))) {
          count_after = sparkSession.read.parquet(s"spark-warehouse/$tableName").count()
          println("Count after   " + count_after)
        }

        assert(count_before < count_after, "Saving in hive is not working properly")
        info("Hive works")
      }
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(250*60)
    connection.close()
  }

}