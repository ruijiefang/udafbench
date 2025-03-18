//https://raw.githubusercontent.com/Clustering4Ever/Clustering4Ever/87870ef8ae266ed99c1ec1ebe70d70a19f78c2b4/distributed/src/main/scala/org/clustering4ever/qualitymeasures/NmiAccumulator.scala
package org.clustering4ever.qualitymeasures

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
/**
 *
 */
final case class NmiAccumulator(initialValue: mutable.ArrayBuffer[mutable.ArrayBuffer[Double]], x: Int, y: Int) extends AccumulatorV2[mutable.ArrayBuffer[mutable.ArrayBuffer[Double]], mutable.ArrayBuffer[mutable.ArrayBuffer[Double]]] {
  /**
   *
   */
  type NmiAccumulatorType = mutable.ArrayBuffer[mutable.ArrayBuffer[Double]]
  /**
   *
   */
  private var nmiAccumulator: NmiAccumulatorType = initialValue
  /**
   *
   */
  final def value = nmiAccumulator
  /**
   *
   */
  final def isZero = value.forall(_.forall(_ == 0D))
  /**
   *
   */
  final def reset: Unit = nmiAccumulator = mutable.ArrayBuffer.fill(x)(mutable.ArrayBuffer.fill(y)(0D))
  /**
   *
   */
  final def add(m1: NmiAccumulatorType): Unit = nmiAccumulator = m1.zip(nmiAccumulator).map{ case (v1, altVectors) => v1.zip(altVectors).map( x => x._1 + x._2 ) }
  /**
   *
   */
  final def copy: AccumulatorV2[NmiAccumulatorType, NmiAccumulatorType] = NmiAccumulator(value, x, y)
  /**
   *
   */
  final def merge(otherAccum: AccumulatorV2[NmiAccumulatorType, NmiAccumulatorType]): Unit = add(otherAccum.value)
  /**
   *
   */ 
  final def addOne(x: Int, y: Int): Unit = nmiAccumulator(x)(y) += 1D
  /**
   *
   */
  final def set(newInitialValue: NmiAccumulatorType) = nmiAccumulator = newInitialValue
}