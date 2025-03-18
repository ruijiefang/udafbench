//https://raw.githubusercontent.com/konradmalik/scala-seed/7f03d6f8eb2d770c4ea939d2e0b3944c0df65b16/spark/src/main/scala/io/github/konradmalik/spark/MapAccumulator.scala
package io.github.konradmalik.spark

import org.apache.spark.util.AccumulatorV2

/**
  * [[Map]] implemented as a Spark Accumulator
  *
  * @param value Object to be used in this accumulator
  * @tparam K Type of keys
  * @tparam V Type of values
  */
class MapAccumulator[K, V](var value: Map[K, V])
    extends AccumulatorV2[(K, V), Map[K, V]] {

  /**
    * Constructor
    */
  def this() = this(Map.empty[K, V])

  override def isZero: Boolean = value.isEmpty

  override def copy(): AccumulatorV2[(K, V), Map[K, V]] =
    new MapAccumulator[K, V](value)

  override def reset(): Unit = value = Map.empty[K, V]

  override def add(v: (K, V)): Unit = value = value + (v._1 -> v._2)

  override def merge(other: AccumulatorV2[(K, V), Map[K, V]]): Unit =
    value = value ++ other.value
}
