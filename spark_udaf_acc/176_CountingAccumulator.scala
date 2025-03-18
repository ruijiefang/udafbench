//https://raw.githubusercontent.com/locationtech-labs/geopyspark/97bcb17a56ed4b4059e2f0dbab97706562cac692/geopyspark-backend/geotrellis/src/main/scala/geopyspark/geotrellis/CountingAccumulator.scala
package geopyspark.geotrellis


import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.{Map => MMap, ArrayBuffer}


class CountingAccumulator extends AccumulatorV2[Int, ArrayBuffer[(Int, Int)]] {
  private val values: ArrayBuffer[(Int, Int)] = ArrayBuffer[(Int, Int)]()

  def copy: CountingAccumulator = {
    val other = new CountingAccumulator
    other.merge(this)
    other
  }

  def add(v: Int): Unit = {
    this.synchronized {
      (v -> 1) +=: values
    }
  }

  def merge(other: AccumulatorV2[Int, ArrayBuffer[(Int, Int)]]): Unit =
    this.synchronized { values ++= other.value }

  def isZero: Boolean = values.isEmpty

  def reset: Unit = this.synchronized { values.clear() }

  def value: ArrayBuffer[(Int, Int)] = values
}
