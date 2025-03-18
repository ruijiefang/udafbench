//https://raw.githubusercontent.com/piotr-kalanski/spark-local/7154cf5759c0bfa9fd7b7f41473091e2f2ce4a1a/src/main/scala/com/datawizards/sparklocal/impl/spark/accumulator/AccumulatorV2APISparkImpl.scala
package com.datawizards.sparklocal.impl.spark.accumulator

import com.datawizards.sparklocal.accumulator.AccumulatorV2API
import org.apache.spark.util.AccumulatorV2

abstract class AccumulatorV2APISparkImpl[IN, OUT](private [sparklocal] val acc: AccumulatorV2[IN, OUT]) extends AccumulatorV2API[IN, OUT] {

  override def isRegistered: Boolean = acc.isRegistered

  override def id: Long = acc.id

  override def name: Option[String] = acc.name

  override def isZero: Boolean = acc.isZero

  override def copyAndReset(): AccumulatorV2API[IN, OUT] = {
    val copyAcc = copy()
    copyAcc.reset()
    copyAcc
  }

  override def reset(): Unit = acc.reset()

  override def add(v: IN): Unit = acc.add(v)

  override def value: OUT = acc.value
}
