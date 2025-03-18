//https://raw.githubusercontent.com/pzhur/bin2longudf/9ba2289c9ef7aa409487bdc904beee7ce67fd931/src/main/scala/gov/census/das/spark/udf/sumArraysDouble.scala
// Implements (in a way that can be compiled into a JAR and then registered and called from pyspark)
// a spark User Defined Aggregating function (UDAF)
// which sums a Spark DataFrame column of the spark type ArrayType(DoubleType()), by element, i.e.
// each element of resulting array is the sum of elements with the same index of the arrays in the column
package gov.census.das.spark.udf

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}


// Aggregator requires zero, reduce, merge, finish, bufferEndoder and outputEncoder methods.
// sumArrayElem is the function actually summing the arrays
// register function exposes the name so that sparkContext can access it to register
object sumArraysDouble extends Aggregator[Array[Double], Array[Double], Array[Double]] {
  override def zero: Array[Double] = Array[Double]()

  override def reduce(buffer: Array[Double], newValue: Array[Double]): Array[Double] = {
    //add a single element to a sum of multiple elements, calculated before
    this.sumArraysElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Array[Double], intermediateValue2: Array[Double]): Array[Double] = {
    //add together two sums of multiple elements
    this.sumArraysElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Array[Double]): Array[Double] = {
    //do nothing at the end, after all arrays are summed into a single array
    reduction
  }

  override def bufferEncoder: Encoder[Array[Double]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Array[Double]] = ExpressionEncoder()

  def sumArraysElem(arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
    if (arr1.length==0) {
      return arr2
    } else if (arr2.length == 0) {
      return arr1
    }
    require(arr1.length == arr2.length, "Arrays must have the same length")
    Array.tabulate(arr1.length)(i => arr1(i) + arr2(i))
  }

  def register(spark: SparkSession, name: String = "sumArraysDouble"): Unit = {
    spark.udf.register(name, functions.udaf(sumArraysDouble))
  }

}
