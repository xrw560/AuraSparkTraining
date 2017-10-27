package cn.ncut.bigdata.scala.base

/**
  * 数组转换
  * yield关键字将原始的数组进行转换会产生一个新的数组，原始的数组不变
  */
object ArrayYieldDemo {

  def main(args: Array[String]): Unit = {
    //定义一个数组
    val arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    //将偶数取出乘以10后再生成一个新的数组
    val res = for (i <- arr if i % 2 == 0) yield i * 10
    println(res.toBuffer)

    //更高级的写法,用着更爽
    //filter是过滤，接收一个返回值为boolean的函数
    //map相当于将数组中的每一个元素取出来，应用传进去的函数
    val r = arr.filter(_ % 2 == 0).map(_ * 10)
    println(r.toBuffer)

    //--------------------------数组常用算法-----------------------------
    println(arr.sum) //求和
    println(arr.max) //最大值
    println(arr.sorted.reverse.toBuffer) //排序

  }
}
