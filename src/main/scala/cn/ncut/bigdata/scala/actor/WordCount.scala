package cn.ncut.bigdata.scala.actor

import java.io.File

import scala.actors.{Actor, Future}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by Administrator on 2017/10/27.
  */
case class SubmitTask(fileName: String)

case object StopTask

case class ResultTask(result: Map[String, Int])

class Task extends Actor {
  override def act(): Unit = {
    loop {
      react {
        case SubmitTask(fileName) => {
          val contents = Source.fromFile(new File(fileName)).mkString
          val arr = contents.split("\r\n")
          //局部combiner
          val result = arr.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.length)
          //          val result = arr.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(.foldLeft(0)(_+_._2))
          sender ! ResultTask(result)
        }

        case StopTask => {
          exit()
        }
      }
    }
  }
}


object WordCount {
  def main(args: Array[String]): Unit = {
    val files = Array("D://words.txt", "D://words.log")
    val replySet = new mutable.HashSet[Future[Any]]
    val resultList = new ListBuffer[ResultTask]

    for (f <- files) {
      val t = new Task
      val replay = t.start() !! SubmitTask(f)
      replySet += replay
    }

    while (replySet.nonEmpty) {
      val toCompute = replySet.filter(_.isSet)
      for (r <- toCompute) {
        val result = r.apply()
        resultList += result.asInstanceOf[ResultTask]
        replySet.remove(r)
      }
      Thread.sleep(100)
    }
    //汇总的功能
    val finalResult = resultList.flatMap(_.result).groupBy(_._1).mapValues(x => x.foldLeft(0)(_ + _._2))
    println(finalResult)
  }
}
