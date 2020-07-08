
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Consumer extends App {

  override def main(args: Array[String]): Unit = {
    val config = Utils.loadConfiguration()
    val tableName = config("tableName")
    val interval = config("interval").toInt
    val topics = Array("newsapi")
    // Set logger to WARN
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.WARN)

    // Starting Spark instance and session
    val conf = new SparkConf().setMaster("local[*]")
      .set("hive.metastore.warehouse.dir", "spark-warehouse")
      .set("spark.sql.catalogImplementation","hive")
      .setAppName("StreamingNews")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    val sparkSession = SparkSession.builder().config(ssc.sparkContext.getConf)
      .enableHiveSupport().getOrCreate()

    // Starting stream
    val kafkaParams = Utils.createKafkaParams()
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    // Extract title and description for every request in NewsAPI
    val records = stream.flatMap(record => Utils.extractValues(record.value))
    records.print()
    val count = records.count()
    count.print()

    // Save information in HBase & Hive
    records.foreachRDD(rdd => {
      val num_records = rdd.count()
      if (num_records > 0) {
        HBase.saveToHBase(tableName, rdd)
        Hive.saveToHive(sparkSession, rdd, tableName)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}