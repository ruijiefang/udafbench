//https://raw.githubusercontent.com/pzhur/bin2longudf/9ba2289c9ef7aa409487bdc904beee7ce67fd931/src/main/scala/gov/census/das/spark/udf/sumArraysLong.scala
// Implements (in a way that can be compiled into a JAR and then registered and called from pyspark)
// a spark User Defined Aggregating function (UDAF)
// which sums a Spark DataFrame column of the spark type ArrayType(LongType()) , by element, i.e.
// each element of resulting array is the sum of elements with the same index of the arrays in the column

package gov.census.das.spark.udf

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

// Aggregator requires zero, reduce, merge, finish, bufferEndoder and outputEncoder methods.
// sumArrayElem is the function actually summing the arrays
// register function exposes the name so that sparkContext can access it to register
object sumArraysLong extends Aggregator[Array[Long], Array[Long], Array[Long]] {
  override def zero: Array[Long] = Array[Long]()

  override def reduce(buffer: Array[Long], newValue: Array[Long]): Array[Long] = {
    //add a single element to a sum of multiple elements, calculated before
    this.sumArraysElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Array[Long], intermediateValue2: Array[Long]): Array[Long] = {
    //add together two sums of multiple elements
    this.sumArraysElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Array[Long]): Array[Long] = {
    //do nothing at the end, after all arrays are summed into a single array
    reduction
  }

  override def bufferEncoder: Encoder[Array[Long]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Array[Long]] = ExpressionEncoder()

  def sumArraysElem(arr1: Array[Long], arr2: Array[Long]): Array[Long] = {
    if (arr1.length==0) {
      return arr2
    } else if (arr2.length == 0) {
      return arr1
    }
    require(arr1.length == arr2.length, "Arrays must have the same length")
    Array.tabulate(arr1.length)(i => arr1(i) + arr2(i))
  }

  def register(spark: SparkSession, name: String = "sumArraysLong"): Unit = {
    spark.udf.register(name, functions.udaf(sumArraysLong))
  }

}
