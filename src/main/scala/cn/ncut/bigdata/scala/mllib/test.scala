package cn.ncut.bigdata.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.{KMeansDataGenerator, LinearDataGenerator}

/**
  * Created by zhouning on 2017/12/19.
  * desc: 
  */
object test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local").setAppName("testColStas")
    val sc = new SparkContext(conf)

    val kMeansRDD = KMeansDataGenerator.generateKMeansRDD(sc,40,5,3,1.0,2)
    val count = kMeansRDD.count()
    println(count)

    val take = kMeansRDD.take(5)
    take.foreach(println)
    LinearDataGenerator
  }

}
