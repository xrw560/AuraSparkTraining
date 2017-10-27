package cn.ncut.bigdata.scala.base

/**
  * 遍历数组
  */
object ForArrayDemo {

  def main(args: Array[String]): Unit = {
    //初始化一个数组
    val arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    //增强for循环
    for (i <- arr) {
      print(i + " ")
    }

    println("--------------------------------------------------")
    //好用的until会生成一个Range
    //reverse是将前面生成的Range反转
    for (i <- (0 until arr.length).reverse) {
      print(arr(i) + " ")
    }
  }
}
