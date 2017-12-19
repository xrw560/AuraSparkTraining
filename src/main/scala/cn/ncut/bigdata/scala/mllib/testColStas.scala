package cn.ncut.bigdata.scala.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhouning on 2017/12/19.
  * desc: 
  */
object testColStas {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local").setAppName("testColStas")
    val sc = new SparkContext(conf)

    val data_path = "data/mllib/sample_stat.txt"
    val data = sc.textFile(data_path).map(_.split("\t")).map(f => f.map(f => f.toDouble))
    val data1 = data.map(f => Vectors.dense(f))
    //计算每列最大值，最小值，平均值，方差值，L1范数，L2范数
    val stat1 = Statistics.colStats(data1)

    println(stat1.max)
    println(stat1.min)
    println(stat1.mean)
    println(stat1.variance)
    println(stat1.normL1)
    println(stat1.normL2)


    //计算Pearson系数、Spearson相关系数
    val corr1 = Statistics.corr(data1,"pearson")
    val corr2 = Statistics.corr(data1,"spearman")
    val x1 = sc.parallelize(Array(1.0,2.0,3.0,4.0))
    val y1 = sc.parallelize(Array(5.0,6.0,6.0,6.0))
    val corr3 = Statistics.corr(x1,y1,"pearson")
    println(corr3)


    //卡方检验
    val v1 = Vectors.dense(43.0,9.0)
    val v2 = Vectors.dense(44.0,4.0)
    val c1 = Statistics.chiSqTest(v1,v2)

  }
}
