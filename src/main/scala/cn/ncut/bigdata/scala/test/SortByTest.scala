package cn.ncut.bigdata.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhouning on 2017/12/19.
  * desc: 
  */
object SortByTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("sort")
    val sc = new SparkContext(conf)

    var str = sc.parallelize(Array((5, "b"), (6, "a"), (1, "f"), (3, "d"), (4, "c"), (2, "e")))
    val str1 = str.sortBy(word => word._1, true) //按第一个数据排序
    val str2 = str.sortBy(word => word._2, true) //按第二个数据排序
    str1.foreach(print)
    str2.foreach(print(_))
  }

}
