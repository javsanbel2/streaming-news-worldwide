import org.apache.kafka.common.serialization.StringDeserializer
import play.api.libs.json.{JsValue, Json}

import scala.io.Source

object Utils {

  // Parsing and processing input information
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

      values
    } catch {
      case e: Exception => Seq()
    }
  }

  // Kafka configuration
  def createKafkaParams(): Map[String, Object] = {
    Map[String, Object](
      "client.dns.lookup" -> "resolve_canonical_bootstrap_servers_only",
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "news",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

  // Load configuration from config.txt
  def loadConfiguration(): Map[String, String] = {
    val config_file = Source.fromFile("../config.txt")
    val lines = config_file.getLines()
    val interval = lines.drop(9).next
    val tableName = lines.drop(1).next
    config_file.close()

    Map[String, String](
      "tableName"-> tableName,
      "interval" -> interval
    )
  }
}
