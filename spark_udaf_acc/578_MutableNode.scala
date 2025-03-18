//https://raw.githubusercontent.com/noahgolmant/Influx/c7d3817b8c90f50c170935177d7476346bff9585/src/main/scala/com/mlab/influx/core/MutableNode.scala
package com.mlab.influx.core

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.AccumulatorV2

import scala.reflect.ClassTag
/**
  * Created by ravi on 4/4/17.
  */

abstract class MutableNode[A, B: ClassTag, C] extends Node[A, B] {
  var initialState: C

  val state: Accumulator[C] = new Accumulator(initialState)
  def update(in: C) : Unit
  def update(inStream: DStream[C]) : Unit = inStream.foreachRDD { rdd => rdd.foreach(update) }
  def reset : Unit = state.reset()

}

object MutableNode {

  def apply[A, B: ClassTag, C](f: (A,C)=>B, g: C=>C, initialVal: C) : MutableNode[A,B,C] = new MutableNode[A,B,C] {
    override def apply(in: A): B = f(in,state.value)
    override def apply(inStream: DStream[A]) : DStream[B] = inStream.map(x=>f(x,state.value))
    override def update(in: C) : Unit = state.add(g(in))

    override var initialState: C = initialVal
  }
}

class Accumulator[C](initialState: C) extends AccumulatorV2[C, C] {
  private var state: C = initialState

  def reset(): Unit = { state = initialState}
  def add(in: C): Unit = {state = in }

  override def copy(): AccumulatorV2[C, C] = new Accumulator(state)

  override def isZero(): Boolean = this.value == initialState

  override def merge(other: AccumulatorV2[C, C]): Unit = { state = other.value }

  override def value: C = state
}
