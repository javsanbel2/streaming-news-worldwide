import java.io.File

import ch.qos.logback.classic.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Hive {
  def main(args: Array[String]): Unit = {
    // Set logger to WARN
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.WARN)
    // Hive
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder.appName("hiveapp").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS news (key INT, value STRING) USING hive")
  }
}
