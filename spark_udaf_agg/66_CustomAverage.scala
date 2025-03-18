//https://raw.githubusercontent.com/esteban-mendoza/UDAFs/16b41017128a3e54549480fb6567b8d1afde9b74/src/main/scala/utils/CustomAverage.scala
package com.bbva.datiocoursework
package utils

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class Average(var sum: Long, var count: Long)

// User Defined Aggregate Functions
// User Defined Functions
/*
  See documentation at:
  https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/expressions/Aggregator.html
 */
object CustomAverage extends Aggregator[Int, Average, Double] {

  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  override def zero: Average = Average(0, 0)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  override def reduce(buffer: Average, data: Int): Average = {
    buffer.sum += data
    buffer.count += 1
    buffer
  }

  // Merge two intermediate values
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // Transform the output of the reduction
  override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count.toDouble

  /*
    To efficiently support domain-specific objects, an Encoder is required.
    The encoder maps the domain specific type T to Spark's internal type system.
    For example, given a class Person with two fields, name (string) and age (int),
    an encoder is used to tell Spark to generate code at runtime to serialize the Person
    object into a binary structure. This binary structure often has much lower memory
    footprint as well as are optimized for efficiency in data processing (e.g. in a columnar format).
    To understand the internal binary representation for data, use the schema function.

    From: https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html

    Documentation of Encoders available in:
    https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Encoders$.html
   */

  // The Encoder for the intermediate value type
  override def bufferEncoder: Encoder[Average] = Encoders.product

  // The Encoder for the final output value type
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
