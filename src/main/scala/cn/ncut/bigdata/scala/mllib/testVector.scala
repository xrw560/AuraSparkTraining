package cn.ncut.bigdata.scala.mllib

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by zhouning on 2017/12/19.
  * desc: 
  */
object testVector {

  def main(args: Array[String]): Unit = {

    val vd: Vector = Vectors.dense(2, 0, 6)
    println(vd(2))

    //建立稀疏向量
    val vs: Vector = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 5, 2, 7))
    println(vs(2))
  }
}
