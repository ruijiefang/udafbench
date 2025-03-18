
case class A extends Aggregator[Option[Double], Array[Long], Array[(String, Long)]], B {
  def reduce(): (Int,Int,Int) = {
    println("Hello, world!")
    println("second line")
    if ("a" == "b") {
      println("fourth line")
    }
    val l = List(3, 2, 1)
    return (1, 2, 3)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val a = new A()
    a.foo()
  }
}
