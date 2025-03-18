//https://raw.githubusercontent.com/NeoZephyr/coffe/21d81f4d9afa5987cee4206a955a5fcdd6404072/data-learning/src/main/scala/com/pain/core/spark/sql/AverageAgg.scala
package com.pain.core.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

object AverageAgg extends Aggregator[Student, Average, Double] {
  override def zero: Average = Average(0, 0)

  override def reduce(b: Average, a: Student): Average = {
    b.sum += a.score
    b.count += 1
    b
  }

  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[Average] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
