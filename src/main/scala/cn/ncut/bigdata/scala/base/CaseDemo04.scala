package cn.ncut.bigdata.scala.base

import scala.util.Random

/**
  * 样例类
  * 在Scala中样例类是一中特殊的类，可用于模式匹配。case class是多例的，后面要跟构造参数，case object是单例的
  */
case class SubmitTask(id: String, name: String)

case class HeartBeat(time: Long)

case object CheckTimeoutTask


object CaseDemo04 {
  def main(args: Array[String]): Unit = {
    val arr = Array(CheckTimeoutTask, HeartBeat(12333), SubmitTask("001", "task-0001"))

    arr(Random.nextInt(arr.length)) match {
      case SubmitTask(id, name) => {
        println(s"$id,$name") //前面需要加上s, $id直接取id的值
      }
      case HeartBeat(time) => {
        println(time)
      }
      case CheckTimeoutTask => {
        println("check")
      }
    }
  }
}
