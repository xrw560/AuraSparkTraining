package cn.ncut.bigdata.scala.base

/**
  * Created by Administrator on 2017/10/27.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val lines = List("hello tom hello jerry", "hello jerry", "hello kitty")
    lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.size).toList.sortBy(_._1).foreach(println)
    lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map(t => (t._1, t._2.size)).toList.sortBy(_._1).foreach(println)
    lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList.sortBy(_._2).foreach(println)
  }
}
