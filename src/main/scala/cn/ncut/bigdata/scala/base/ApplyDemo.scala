package cn.ncut.bigdata.scala.base

/**
  * apply方法
  * 通常我们会在类的伴生对象中定义apply方法，当遇到类名(参数1,...参数n)时apply方法会被调用
  */
object ApplyDemo {

  def main(args: Array[String]): Unit = {
    //调用了Array伴生对象的apply方法
    //def apply(x: Int, xs: Int*): Array[Int]
    //arr1中只有一个元素5
    val arr1 = Array(5)
    println(arr1.toBuffer)

    //new了一个长度为5的array，数组里面包含5个null
    val arr2 = new Array(5)
  }
}
