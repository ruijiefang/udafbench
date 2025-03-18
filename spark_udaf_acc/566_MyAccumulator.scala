//https://raw.githubusercontent.com/WFY123wfy/algo_data_lab/bfb28f3ac2fb6944c6554e6723775f69ad3f5d65/spark/src/main/java/com/example/demo/MyAccumulator.scala
package com.example.demo

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

class MyAccumulator extends AccumulatorV2[String, ArrayBuffer[String]]{
  private var result = ArrayBuffer[String]()

  // 返回该累加器的值是否为“零”
  override def isZero: Boolean = this.result.isEmpty

  // 指定对自定义累加器的复制操作
  override def copy(): AccumulatorV2[String, ArrayBuffer[String]] = {
    val newAccum = new MyAccumulator
    newAccum.result = this.result
    newAccum
  }

  // 重置累加器
  override def reset(): Unit = this.result = new ArrayBuffer[String]()

  // 指定元素相加操作
  override def add(v: String): Unit = this.result += v

  // 合并两个相同类型的累加器
  override def merge(other: AccumulatorV2[String, ArrayBuffer[String]]): Unit = {
    result.++=:(other.value) // ArrayBuffer的一个方法，用于将另一个集合的所有元素添加到当前集合的开头
  }

  // 返回累加器当前的值
  override def value: ArrayBuffer[String] = this.result
}