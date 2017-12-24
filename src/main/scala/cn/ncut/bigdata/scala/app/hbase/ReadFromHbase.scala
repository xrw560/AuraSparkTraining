package cn.ncut.bigdata.scala.app.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableDescriptors, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object ReadFromHbase {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ReadFromHbase").setMaster("local")

    val sc = new SparkContext(sparkConf)

    val tableName = "account"
    val conf = HBaseConfiguration.create()
    //设置zookeeper集群地址，也可以通过将hbase-shell.xml导入classpath,但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "bigdata")
    // 设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    //如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    //读取数据并转化为rdd
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val count = hbaseRDD.count()
    println("count: " + count)
    hbaseRDD.foreach { case (_, result) => {
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val c1 = Bytes.toString(result.getValue("cf".getBytes, "c1".getBytes))
      println("Row Key: " + key + " c1: " + c1)
    }
    }

    sc.stop()
    admin.close()
  }

}
