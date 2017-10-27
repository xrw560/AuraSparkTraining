package cn.ncut.bigdata.scala.actor

import scala.actors.Actor

/**
  * 分别调用了两个单例对象的start()方法，
  * 他们的act()方法会被执行，相当于在java中开启了两个线程，线程的run()方法会被执行
  */
object MyActor1 extends Actor {
  //重新act方法
  override def act(): Unit = {
    for (i <- 1 to 10) {
      println("actor-1 " + i)
      Thread.sleep(2000)
    }
  }
}

object MyActor2 extends Actor {
  //重新act方法
  override def act(): Unit = {
    for (i <- 1 to 10) {
      println("actor-2 " + i)
      Thread.sleep(2000)
    }
  }
}

object ActorTest {
  def main(args: Array[String]): Unit = {
    //启动Actor
    MyActor1.start()
    MyActor2.start()
  }
}
