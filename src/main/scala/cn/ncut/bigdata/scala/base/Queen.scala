package cn.ncut.bigdata.scala.base

/**
  * 构造器参数可以不带val或var，如果不带val或var的参数至少被一个方法所使用，
  * 那么它将会被提升为字段
  */
class Queen private(val name: String, prop: Array[String], private var age: Int = 18) {

  println(prop.size)


  def description = name + " is " + age + " years old with " + prop.toBuffer
}

object Queen {
  def main(args: Array[String]): Unit = {
    val q = new Queen("hatano", Array("蜡烛", "皮鞭"), 20)
    println(q.description)
  }
}