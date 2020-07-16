import java.util.Date

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, ConnectionFactory, HBaseAdmin, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes

import scala.io.Source
import play.api.libs.json._

object Testing {
  def main(args: Array[String]): Unit = {
    val crcText = Source.fromFile("../tests_output/logCrc.txt")
    val lines = crcText.getLines()
    println(lines.drop(1).next())
    //    println(Some(values))
//    println(descriptions(0))
//    val he:Seq[Int] = Seq(1,2,3)
//    val holaa = he.map(_ * 2)
//    println(holaa)
  }
}
