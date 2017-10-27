package cn.ncut.bigdata.scala.base

/**
  * 元组
  */
object Tuple {

  def main(args: Array[String]): Unit = {
    //定义元组时用小括号将多个元素包起来，元素之间用逗号分隔
    //元素的类型可以不一样，元素个数可以任意多个
    val t = ("hadoop", 3.14, 65535)

    val t2, (a, b, c) = ("hadoop", 3.14, 65535)
    println(t2 + " " + a + " " + b + " " + c)

    //获取元组中的元素可以使用下划线加脚标
    //但是需要注意的是元组中的元素脚标是从1开始的
    val r1 = t2._1
    println(r1)
    val r2 = t2._2
    println(r2)

    //--------------------------将对偶的集合转换成映射---------------------------
    val arr = Array(("tom", 88), ("jerry", 90))
    println(arr.toMap) //toMap可以将对偶的集合转换为映射

    //--------------------------拉链操作---------------------------
    //注意：如果两个数组的元素个数不一致，拉链操作后生成的数组的长度为较小的那个数组的元素个数
    val names = Array("tom", "jerry", "kitty")
    val scores = Array(88, 95, 80)
    val ns = names.zip(scores)
    println(ns.toMap)

  }

}
