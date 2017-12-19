package cn.ncut.bigdata.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by zhouning on 2017/12/19.
  * desc: 
  */
object testPearson {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local").setAppName("testColStas")
    val sc = new SparkContext(conf)

    val data_path = "data/mllib/sample_stat.txt"
    val data = sc.textFile(data_path).map(_.split("\t")).map(f => f.map(f => f.toDouble))
    val data1 = data.map(f => Vectors.dense(f))

  }

}
