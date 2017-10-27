package cn.ncut.bigdata.scala.actor

import scala.actors.Actor

/**
  * 说明：在act()方法中加入了while (true) 循环，就可以不停的接收消息
  * 注意：发送start消息和stop的消息是异步的，但是Actor接收到消息执行的过程是同步的按顺序执行
  */
class MyActor extends Actor {
  override def act(): Unit = {
    while (true) {
      receive {
        case "start" => {
          println("starting....")
          Thread.sleep(5000)
          println("started.....")
        }
        case "stop" => {
          println("stopping...")
          Thread.sleep(5000)
          println("stopped......")
        }
      }
    }
  }
}

object MyActor {
  def main(args: Array[String]): Unit = {
    val actor = new MyActor
    actor.start()
    actor ! "start"
    actor ! "stop"
    println("消息发送完成")
  }
}