//https://raw.githubusercontent.com/guillepena/scala-portfolio/bcf19fe7f27be6db4ba85a8f4c66723c0f64203e/src/main/scala/spark/sql/uda/main/UDAExample01.scala
package eoi.de.examples
package spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.expressions.Aggregator

final case class ValueWithCount(value: Double, count: Int)

object ExampleFunctionsUDA01 {
  object Percentile
      extends Aggregator[ValueWithCount, Map[Int, Double], Map[Int, Double]] {
    def zero: Map[Int, Double] = Map.empty[Int, Double]

    def reduce(
        buffer: Map[Int, Double],
        valueCount: ValueWithCount,
    ): Map[Int, Double] = buffer +
      (valueCount.value.toInt -> valueCount.count.toDouble)

    def merge(b1: Map[Int, Double], b2: Map[Int, Double]): Map[Int, Double] =
      mergeMaps(b1, b2)

    def finish(reduction: Map[Int, Double]): Map[Int, Double] =
      normalizeMap(reduction)

    def bufferEncoder: Encoder[Map[Int, Double]] = Encoders
      .kryo[Map[Int, Double]]

    def outputEncoder: Encoder[Map[Int, Double]] = Encoders
      .kryo[Map[Int, Double]]

    private def mergeMaps(
        map1: Map[Int, Double],
        map2: Map[Int, Double],
    ): Map[Int, Double] = (map1.toSeq ++ map2.toSeq).groupBy(_._1).view
      .mapValues(_.map(_._2).sum).toMap

    private def normalizeMap(map: Map[Int, Double]): Map[Int, Double] = {
      val totalValueCount = map.values.sum
      map.map { case (value, count) => value -> count / totalValueCount }
    }
  }
}

object UDAExample01 extends App {

  val spark = SparkSession.builder().appName("ComplexUDAExample")
    .master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val data = Seq((1.0, 2), (2.0, 3), (3.0, 4)).toDF("value", "count")
  data.as[ValueWithCount].show(false)

  val percentiles: TypedColumn[ValueWithCount, Map[Int, Double]] =
    ExampleFunctionsUDA01.Percentile.toColumn.name("percentiles")
  val percentilesDF: DataFrame = data.as[ValueWithCount].groupByKey(_ => true)
    .agg(percentiles).select("percentiles")
  percentilesDF.show(truncate = false)
}
