//https://raw.githubusercontent.com/esteban-mendoza/UDAFs/16b41017128a3e54549480fb6567b8d1afde9b74/src/main/scala/utils/Correlation.scala
package com.bbva.datiocoursework
package utils

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class CorrAccumulator(var crossed: Double, var xSquared: Double, var ySquared: Double)

object Correlation extends Aggregator[(Int, Double, Int, Double), CorrAccumulator, Double] {
  
  override def zero: CorrAccumulator = CorrAccumulator(0, 0, 0)

  override def reduce(b: CorrAccumulator, a: (Int, Double, Int, Double)): CorrAccumulator = {
    b.crossed += (a._1.toDouble - a._2) * (a._3.toDouble - a._4)
    b.xSquared += Math.pow(a._1.toDouble - a._2, 2)
    b.ySquared += Math.pow(a._3.toDouble - a._4, 2)
    b
  }

  override def merge(b1: CorrAccumulator, b2: CorrAccumulator): CorrAccumulator = {
    b1.crossed += b2.crossed
    b1.xSquared += b2.xSquared
    b1.ySquared += b2.ySquared
    b1
  }

  override def finish(reduction: CorrAccumulator): Double = {
    reduction.crossed / Math.sqrt(reduction.xSquared * reduction.ySquared)
  }

  override def bufferEncoder: Encoder[CorrAccumulator] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
