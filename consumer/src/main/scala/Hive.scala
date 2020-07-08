import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Hive {
  // Method to save to Hive
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
}
