package cn.ncut.bigdata.scala.mllib

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhouning on 2017/12/19.
  * desc: 
  */
object testLabeledPoint2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("testLabeledPoint2")
    val sc = new SparkContext(conf)

    val mu = MLUtils.loadLibSVMFile(sc, "d://sample_libsvm_data.txt")
    mu.take(1).foreach(println)
  }
}
