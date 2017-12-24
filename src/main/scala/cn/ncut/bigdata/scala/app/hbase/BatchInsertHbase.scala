package cn.ncut.bigdata.scala.app.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object BatchInsertHbase {


  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("hbase"))
    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 to 10000)
    rdd.map(value => {
      var put = new Put(Bytes.toBytes(value.toString))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("c1"), Bytes.toBytes(value.toString))
      put
    }).foreachPartition(iterator => {
      var jobConf = new JobConf(HBaseConfiguration.create())
      jobConf.set("hbase.zookeeper.quorum", "bigdata")
      jobConf.set("zookeeper.znode.parent", "/hbase")
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      val table = new HTable(jobConf, TableName.valueOf("account"))
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(iterator.toSeq))
    })

  }
}
