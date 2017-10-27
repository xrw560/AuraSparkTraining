package cn.ncut.bigdata.scala.base

/**
  * 映射
  * 在Scala中，把哈希表这种数据结构叫做映射
  */
object MapDemo {

  def main(args: Array[String]): Unit = {
    val scores = Map("tom" -> 85, "jerry" -> 99, "kitty" -> 90)
    val scores2 = Map(("tom", 85), ("jerry", 99), ("kitty", 90))

    println(scores("jerry"))

    println(scores.getOrElse("suke", 0))

    println("----------------------可变Map---------------------")

    import scala.collection.mutable.Map
    //val定义的scores3变量意味着变量的引用不变，但是Map的内容可变
    val scores3 = Map("tom" -> 80, "jerry" -> 99)
    scores3("tom") = 88 //修改map中的内容
    scores3 += ("kitty" -> 90, "suke" -> 60) //用+=向原来的map中追加元素


  }
}
