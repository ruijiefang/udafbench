
case class A extends Aggregator[Option[Double], Array[Long], Array[(String, Long)]], B {
  def foo(): Unit = {
    println("Hello, world!")
    println("second line")
    if ("a" == "b") {
      println("fourth line")
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val a = new A()
    a.foo()
  }
}
