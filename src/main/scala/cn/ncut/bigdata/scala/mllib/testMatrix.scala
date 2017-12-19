package cn.ncut.bigdata.scala.mllib

import org.apache.spark.mllib.linalg.Matrices

/**
  * Created by zhouning on 2017/12/19.
  * desc: 
  */
object testMatrix {

  def main(args: Array[String]): Unit = {
    val mx = Matrices.dense(2, 3, Array(1, 2, 3, 4, 5, 6))
    println(mx)
  }
}
