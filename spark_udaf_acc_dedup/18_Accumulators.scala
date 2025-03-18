//https://raw.githubusercontent.com/apktool/LearnSpark/ff04cbe4d6638ad032ec7a343c902ea6757d3d28/spark-demo/src/main/scala/com/spark/Accumulators.scala
package com.spark

import java.util.Collections

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

// This example demonstrates accumulators that allow you to build global state
// safely and efficiently during parallel computations on RDDs.
//
// It begins with a simple "out of the box" accumulator and then covers two
// more complex ones that require you to extend the AccumulatorV2 class.

object Accumulators {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulators").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val words = sc.parallelize(Seq("Fred", "Bob", "Francis", "James", "Frederick", "Frank", "Joseph"), 4)
    val count = sc.longAccumulator

    // Example 1: use a simple counter to keep track of the number of words starting with "F"
    // an efficient counter -- this is one of the basic accumulators that Spark provides "out of the box"

    println("*** Using a simple counter")
    words.filter(_.startsWith("F")).foreach(n => count.add(1))
    println("total count of names starting with F = " + count.value)

    // Example 2: Accumulate the set of words starting with "F" -- you can always count them later
    // efficiently accumulate a set -- there's now "out of the box" way to do this

    val names = new StringSetAccumulator
    sc.register(names)

    println("*** using a set accumulator")
    words.filter(_.startsWith("F")).foreach(names.add)
    println("All the names starting with 'F' are a set")
  }

  class StringSetAccumulator extends AccumulatorV2[String, java.util.Set[String]] {
    private val _set = Collections.synchronizedSet(new java.util.HashSet[String]())

    override def isZero: Boolean = _set.isEmpty

    override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
      val newAcc = new StringSetAccumulator()
      newAcc._set.addAll(_set)
      newAcc
    }

    override def reset(): Unit = _set.clear()

    override def add(v: String): Unit = _set.add(v)

    override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit = {
      _set.addAll(other.value)
    }

    override def value: java.util.Set[String] = _set
  }

}
