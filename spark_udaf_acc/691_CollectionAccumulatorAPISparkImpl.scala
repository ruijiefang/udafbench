//https://raw.githubusercontent.com/piotr-kalanski/spark-local/7154cf5759c0bfa9fd7b7f41473091e2f2ce4a1a/src/main/scala/com/datawizards/sparklocal/impl/spark/accumulator/CollectionAccumulatorAPISparkImpl.scala
package com.datawizards.sparklocal.impl.spark.accumulator

import com.datawizards.sparklocal.accumulator.{AccumulatorV2API, CollectionAccumulatorAPI}
import org.apache.spark.util.AccumulatorV2

class CollectionAccumulatorAPISparkImpl[T](acc: AccumulatorV2[T, java.util.List[T]])
  extends AccumulatorV2APISparkImpl[T, java.util.List[T]](acc)
    with CollectionAccumulatorAPI[T] {

  override def copyAndReset(): AccumulatorV2API[T, java.util.List[T]] =
    new CollectionAccumulatorAPISparkImpl(acc.copyAndReset())

  override def copy(): AccumulatorV2API[T, java.util.List[T]] =
    new CollectionAccumulatorAPISparkImpl(acc.copy())

  override def merge(other: AccumulatorV2API[T, java.util.List[T]]): Unit = other match {
    case a:CollectionAccumulatorAPISparkImpl[T] => acc.merge(a.acc)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }
}
