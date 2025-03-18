//https://raw.githubusercontent.com/konradmalik/scala-seed/7f03d6f8eb2d770c4ea939d2e0b3944c0df65b16/spark/src/main/scala/io/github/konradmalik/spark/ConcurrentSkipListSetAccumulator.scala
package io.github.konradmalik.spark

import java.util.concurrent.ConcurrentSkipListSet

import org.apache.spark.util.AccumulatorV2

/**
  * [[ConcurrentSkipListSet]] implemented as a Spark Accumulator
  *
  * @param value Object to be used in this accumulator
  * @tparam T Type of the accumulated values
  */
class ConcurrentSkipListSetAccumulator[T](
    override val value: ConcurrentSkipListSet[T]
) extends AccumulatorV2[T, ConcurrentSkipListSet[T]] {

  /**
    * Constructor
    */
  def this() = this(new ConcurrentSkipListSet[T]())

  override def isZero: Boolean = value.isEmpty

  override def copy() = new ConcurrentSkipListSetAccumulator(value)

  override def reset(): Unit = value.clear()

  override def add(v: T): Unit = value.add(v)

  override def merge(
      other: AccumulatorV2[T, ConcurrentSkipListSet[T]]
  ): Unit = {
    val iterator = other.value.iterator()
    while (iterator.hasNext) {
      value.add(iterator.next())
    }
  }
}
