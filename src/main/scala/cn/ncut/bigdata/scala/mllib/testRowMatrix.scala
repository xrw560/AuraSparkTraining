package cn.ncut.bigdata.scala.mllib

import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

/**
  * Created by zhouning on 2017/12/19.
  * desc: 
  */
object testRowMatrix {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("testRowMatrix")
    val sc = new SparkContext(conf)
    val z = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3)))
    val key = z.aggregateByKey(0)(math.max(_, _), _ + _)
    key.foreach(println)
  }
}
