package cn.ncut.bigdata.scala.base

/**
  * 方法和函数
  */
object MethodAndFunctionDemo {
  /**
    * 定义方法
    * 定义方法用def关键字
    * m1是方法名称
    * x,y是参数列表
    * Int是返回值类型
    * x*y是方法体
    */
  def method(x: Int, y: Int): Int = x * y

  /**
    * 定义函数
    */
  val func = (x: Int, y: Int) => x + y
  val func1: Int => String = { x => x.toString }
  val func2 = (x: Int) => x.toString

  //  --------------------------------------------------------------------

  //定义一个方法
  //方法m2参数要求是一个函数，函数的参数必须是两个Int类型
  //返回值类型也是Int类型
  def m1(f: (Int, Int) => Int): Int = {
    f(2, 6)
  }

  //定义一个函数f1，参数是两个Int类型，返回值是一个Int类型
  val f1 = (x: Int, y: Int) => x + y
  //再定义一个函数f2
  val f2 = (m: Int, n: Int) => m * n

  //  ---------------------------将方法转换成函数（神奇的下划线）-----------------------------------------
  def m2(x: Int, y: Int): Int = x + y

  val f3 = m2 _

  def main(args: Array[String]): Unit = {
    //调用m1方法，并传入f1函数
    val r1 = m1(f1)
    println(r1)

    //调用m1方法，并传入f2函数
    val r2 = m1(f2)
    println(r2)

  }
}
