package cn.ncut.bigdata.scala.app.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object MapReduceTaskForHbase {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("hbase"))
    val conf = HBaseConfiguration.create()
    var jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.quorum","bigdata")
    jobConf.set("zookeeper.znode.parent","/hbase")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"account")
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 to 1000)
    rdd.map(x=>{
      var put = new Put(Bytes.toBytes(x.toString))
      put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("c1"),Bytes.toBytes(x.toString))
      (new ImmutableBytesWritable,put)
    }).saveAsHadoopDataset(jobConf)

  }

}
