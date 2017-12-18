package cn.ncut.bigdata.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhouning on 2017/12/19.
  */
object FlatMapTest {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("flatmap")
    val sc = new SparkContext(conf)
    val arr = sc.parallelize(Array(1, 2, 3, 4, 5, 6))
    val result = arr.flatMap(x => List(x + 1)).collect()
    result.foreach(println)
  }

}
