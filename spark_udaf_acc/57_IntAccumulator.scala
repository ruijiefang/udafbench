//https://raw.githubusercontent.com/TresAmigosSD/SMV/e12257b5b07113d805e7fdd8de41cbcf72120ed7/src/main/scala/org/tresamigos/smv/util/IntAccumulator.scala
package org.tresamigos.smv.util

import org.apache.spark.util.AccumulatorV2

class IntAccumulator extends AccumulatorV2[Int, Int] {
  private var _sum: Int = 0

  def isZero: Boolean = _sum == 0

  def copy(): IntAccumulator = {
    val newAcc = new IntAccumulator
    newAcc._sum = this._sum
    newAcc
  }

  def reset(): Unit = {
    _sum = 0
  }

  def add(v: Int): Unit = {
    _sum += v
  }

  def sum: Int = _sum

  def merge(other: AccumulatorV2[Int, Int]): Unit = other match {
    case o: IntAccumulator =>
      _sum += o.sum
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Int = _sum
}
