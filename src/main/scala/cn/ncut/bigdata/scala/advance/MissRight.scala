package cn.ncut.bigdata.scala.advance

/**
  * Created by Administrator on 2017/10/27.
  */
object ImplicitContext {

  implicit object OrderingGirl extends Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = if (x.faceValue > y.faceValue) 1 else -1
  }

}

class Girl(var name: String, var faceValue: Double) {
  override def toString: String = s"name: $name, faceValue: $faceValue"
}


class MissRight[T: Ordering](val f: T, val s: T) {
  def choose()(implicit ord: Ordering[T]) = if (ord.gt(f, s)) f else s
}

object MissRight {
  def main(args: Array[String]): Unit = {
    import ImplicitContext._
    val g1 = new Girl("yuihatano", 99)
    val g2 = new Girl("jzmb", 98)
    val mr = new MissRight(g1, g2)
    val result = mr.choose()
    println(result)
  }
}

