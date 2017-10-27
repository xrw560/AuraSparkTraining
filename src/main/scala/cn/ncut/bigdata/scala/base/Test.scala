package cn.ncut.bigdata.scala.base

/**
  * Created by Administrator on 2017/10/27.
  */
object Test {

  def main(args: Array[String]): Unit = {
    //创建一个List
    val lst0 = List(1, 7, 9, 8, 0, 3, 5, 4, 6, 2)

    //将lst0中每个元素乘以10后生成一个新的集合
    lst0.map(_ * 10)

    //将lst0中的偶数取出来生成一个新的集合
    lst0.filter(x => x % 2 == 0)

    //将lst0排序后生成一个新的集合
    lst0.sorted

    //反转顺序
    lst0.sorted.reverse

    //将lst0中的元素4个一组,类型为Iterator[List[Int]]
    lst0.grouped(4)

    //将Iterator转换成List
    lst0.grouped(4).toList

    //将多个list压扁成一个List
    lst0.grouped(4).toList.flatten

    val lines = List("hello tom hello jerry", "hello jerry", "hello kitty")
    //先按空格切分，在压平
    val result2 = lines.flatMap(_.split(" "))


    //并行计算求和
    lst0.par

    //化简：reduce
    lst0.par.reduce(_ + _)

    //将非特定顺序的二元操作应用到所有元素
    //reduce -> reduceLeft

    //按照特定的顺序


    //折叠：有初始值（无特定顺序）
    lst0.par.fold(0)(_ + _)

    //折叠：有初始值（有特定顺序）
    lst0.foldLeft(0)(_ + _)


    //聚合
    val arr = List(List(1, 2, 3), List(3, 4, 5), List(2), List(0))
    val result = arr.aggregate(0)(_ + _.sum, _ + _)

    //---------------------------------------------------------
    val l1 = List(5, 6, 4, 7)
    val l2 = List(1, 2, 3, 4)
    //求并集
    val r1 = l1.union(l2)

    //求交集
    val r2 = l1.intersect(l2)

    //求差集
    val r3 = l1.diff(l2)

    println(r3)
    //--------------------------------------------------------
    val lines2 = List("hello tom hello jerry", "hello jerry", "hello kitty")
    lines2.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))
    lines2.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map(t => (t._1, t._2.size)).toList.sortBy(_._2).reverse

  }
}
