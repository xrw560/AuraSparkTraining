package cn.ncut.bigdata.scala.base

import scala.collection.mutable

/**
  * Created by Administrator on 2017/10/27.
  */
object MutMapDemo {
  def main(args: Array[String]): Unit = {
    val map1 = new mutable.HashMap[String, Int]()
    //向map中添加数据
    map1("spark") = 1
    map1 += (("hadoop", 2))
    map1.put("storm", 3)
    println(map1)

    //从map中移除元素
    map1 -= "spark"
    map1.remove("storm")
    println(map1)
  }
}
