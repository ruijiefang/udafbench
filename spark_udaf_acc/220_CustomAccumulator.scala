//https://raw.githubusercontent.com/thedevd/techBlog/4e4bf2de7f9faa4a06401c2b0627c36d9b8c5cd9/sparkexamples/src/main/scala/com/thedevd/sparkexamples/sharedvariables/CustomAccumulator.scala
package com.thedevd.sparkexamples.sharedvariables

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

/*
 * 1. Some times spark built in provided accumulators (longAccumulator, doubleAccumulator or
 *    collectionAccumulator) does not meet the requirement of the business, so we need to
 *    use custom accumulator provides by spark.
 *
 * 2. The new AccumulatorV2 API from spark 2.x enables you to define clean custom accumulators.
 *    Although spark 1.x has feature of creating custome accumulator but this is very much troublesome to use.
 *    Abstract class: AccumulatorV2 in spark 2.x provides a more friendly implementation of custom type accumulators.
 *
 * 3. Principle Steps to create custom accumulator in spark 2.x (only two steps needed) =>
 *    a. The custom accumulator needs to inherit AccumulatorV2 [type, type], and implement all methods.
 *    b. register the accumulator with sparkContext so that driver can operate it as shared variable.
 *
 */

object CustomAccumulator {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("CustomAccumulator")
      .master("local[*]")
      .getOrCreate()

    // In this we try to find sum of the numbers in RDD using built in and custom accumulator
    usingLongAccumulator(spark)
    usingCustomAccumulator(spark)
  }

  def usingLongAccumulator(spark: SparkSession) = {

    val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 4)
    val sum = spark.sparkContext.longAccumulator

    rdd.map(number => {
      sum.add(number)
    }).collect() // calling action is must to trigger the execution

    println("Sum using longAccumulator: " + sum.value) // Sum using longAccumulator: 55
  }

  def usingCustomAccumulator(spark: SparkSession) = {

    val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 4)
    val customAccumulator = new MySumAccumulator
    
    // custom Accumulator must be registered at driver side before sending to executor
    spark.sparkContext.register(customAccumulator, "customSumAccumulator")

    rdd.map(number => {
      customAccumulator.add(number)
    }).collect() // calling action is must to trigger the execution

    println("Sum using custom accumulator: " + customAccumulator.value)
    // Sum using custom accumulator: 55
  }

  // Due to problem in eclipse scala plugin the class is defined within Object
  class MySumAccumulator extends AccumulatorV2[Int, Int] {
    // AccumulatorV2 has 6 unimplemented members.
    var sum = 0

    def add(v: Int): Unit = {
      sum = sum + v
    }

    def copy(): org.apache.spark.util.AccumulatorV2[Int, Int] = {
      val newMySumAccumulator = new MySumAccumulator
      newMySumAccumulator
    }

    def isZero: Boolean = { sum == 0 }

    def merge(other: org.apache.spark.util.AccumulatorV2[Int, Int]): Unit = {
      // Merge is used by driver to merge the current value and the value returned by executors after execution
      sum = sum + other.value
    }
    def reset(): Unit = { sum = 0 }

    def value: Int = { sum }
  }

}
