import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

object HBase {
  // Method to save in HBase
  def saveToHBase(tableName:String, rdd: RDD[(String, String, String)]): Unit = {
    val connection = ConnectionFactory.createConnection(createConfHBase())
    val table = connection.getTable(TableName.valueOf(tableName))

    rdd.collect().foreach( row => {
      val put = new Put(Bytes.toBytes(row._3))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("title"), Bytes.toBytes(row._1))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("description"), Bytes.toBytes(row._2))

      table.put(put)
    })

    table.close()
    connection.close()
  }

  // HBase configuration
  def createConfHBase(): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost:2181")
    conf.set("hbase.master", "localhost:60000")
    conf.set("hbase.rootdir", "file:///tmp/hbase")
    conf
  }
}
