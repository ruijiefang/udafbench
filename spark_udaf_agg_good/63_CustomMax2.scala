//https://raw.githubusercontent.com/nkudinov/udaf/ec4007cb1eefa6b567b7e33f7f0b8470691b45d2/src/main/scala/CustomMax2.scala
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

case class CustomMax2(col: String)
  extends Aggregator[Row, Int, Int] with Serializable {

  def zero = Int.MinValue
  def reduce(acc: Int, x: Row) =
    Math.max(acc, Option(x.getAs[Int](col)).getOrElse(zero))

  def merge(acc1: Int, acc2: Int) = Math.max(acc1, acc2)
  def finish(acc: Int) = acc

  def bufferEncoder: Encoder[Int] = Encoders.scalaInt
  def outputEncoder: Encoder[Int] = Encoders.scalaInt
}