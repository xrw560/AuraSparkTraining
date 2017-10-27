package cn.ncut.bigdata.scala.base

import scala.collection.immutable.HashSet


/**
  * 不可变的Set
  */
object ImmutSetDemo {

  def main(args: Array[String]): Unit = {
    val set1 = new HashSet[Int]()
    val set2 = set1 + 4

    val set3 = set1 ++ Set(5, 6, 7)
    val set0 = Set(1, 2, 3) ++ set1
    println(set0.getClass)
  }
}
