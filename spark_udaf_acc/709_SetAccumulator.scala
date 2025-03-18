//https://raw.githubusercontent.com/konradmalik/scala-seed/7f03d6f8eb2d770c4ea939d2e0b3944c0df65b16/spark/src/main/scala/io/github/konradmalik/spark/SetAccumulator.scala
package io.github.konradmalik.spark

import org.apache.spark.util.AccumulatorV2

/**
  * [[Set]] implemented as a Spark Accumulator
  *
  * @param init initialization of the accumulator
  * @tparam T Type of values
  */
class SetAccumulator[T](init: Set[T]) extends AccumulatorV2[T, Set[T]] {

  /**
    * Internal collection
    */
  private var _value: Set[T] = init

  /**
    * Constructor
    */
  def this() = this(Set.empty[T])

  override def value: Set[T] = _value

  override def isZero: Boolean = value.isEmpty

  override def copy(): AccumulatorV2[T, Set[T]] = new SetAccumulator[T](value)

  override def reset(): Unit = _value = Set.empty[T]

  override def add(v: T): Unit = _value = _value + v

  override def merge(other: AccumulatorV2[T, Set[T]]): Unit =
    _value = _value ++ other.value
}
