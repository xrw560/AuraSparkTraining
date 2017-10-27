package cn.ncut.bigdata.scala.base

import scala.util.Random

/**
  * 匹配字符串
  */
object CaseDemo01 {
  def main(args: Array[String]): Unit = {

    //--------------------------匹配字符串--------------------------------
    val arr = Array("YoshizawaAkiho", "YuiHatano", "AoiSola")
    val name = arr(Random.nextInt(arr.length))
    name match {
      case "YoshizawaAkiho" => println("吉泽老师...")
      case "YuiHatano" => println("波多老师...")
      case _ => println("真不知道你们在说什么...")
    }

  }
}
