package cn.ncut.bigdata.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhouning on 2017/12/19.
  * desc: 寻找最长字符串
  */
object ReduceTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建环境变量
      .setMaster("local") //设置本地化处理
      .setAppName("reduce") //设置名称
    val sc = new SparkContext(conf) //创建环境变量实例
    val str = sc.parallelize(Array("one", "two", "three", "four", "five"))
    val result = str.reduce(myFun) //进行数据拟合
    result.foreach(print) //打印结果
    println()
  }

  def myFun(str1: String, str2: String): String = {
    //创建方法
    var str = str1 //设置确定方法
    if (str2.size >= str.size) { //比较方法
      str = str2 //替换
    }
    return str //返回最长的那个字符串
  }
}
