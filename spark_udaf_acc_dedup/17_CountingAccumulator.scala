//https://raw.githubusercontent.com/Open-EO/openeo-geotrellis-extensions/40a9bbd94fe339e88075fb708b2bad727d5f71ce/geopyspark-geotrellis/src/main/scala/geopyspark/geotrellis/CountingAccumulator.scala
package geopyspark.geotrellis


import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer


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
