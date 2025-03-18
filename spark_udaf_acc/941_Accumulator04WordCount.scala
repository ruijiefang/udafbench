//https://raw.githubusercontent.com/rilweic/my-example-code/9f0a13627201841e1cca02dac4e3dcb6120ff56b/spark-demo/src/main/scala/com/lichao666/sparknote/core/Accumulator04WordCount.scala
package com.lichao666.sparknote.core

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Accumulator04WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulator04WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello world", "hello spark", "spark scala", "hello scala"))

    val accumulator = new MyAccumulator
    sc.register(accumulator, "wordcount")

    rdd.flatMap(_.split(" ")).foreach(word => {
      accumulator.add(word)
    })

    accumulator.value.foreach(println)

    sc.stop()
  }
}

class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

  private var wcMap = mutable.Map[String, Int]()

  override def isZero: Boolean = {

    wcMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {

    new MyAccumulator
  }

  override def reset(): Unit = {
    wcMap.clear()
  }

  override def add(v: String): Unit = {
    wcMap.update(v, wcMap.getOrElse(v, 0) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    other.value.foreach {
      case (k, v) => {
        wcMap.update(k, wcMap.getOrElse(k, 0) + v)
      }
    }

  }

  override def value: mutable.Map[String, Int] = wcMap
}