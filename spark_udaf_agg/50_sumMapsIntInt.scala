//https://raw.githubusercontent.com/pzhur/bin2longudf/9ba2289c9ef7aa409487bdc904beee7ce67fd931/src/main/scala/gov/census/das/spark/udf/sumMapsIntInt.scala
// Implements Spark UDAF (user defined aggregator function) that can be complied to JAR and called
// from pyspark for summing a column of sparse matrices (in COO format) converted to python dicts {int: int}
// and then spark MapType(IntType(), IntType()) (see unit tests in python folder).
// The use case was calling from pyspark to sum scipy.sparse.coo matrices, but it didn't show performance increase
// vs summing within python in RDD.
// A better way to use spark functions collect_list followed by flatten, and then convert back to COO at the end,
// summing the duplicate elements
// This function can be used though if a column of MapType(IntType(), IntType()) needs to be summed in some other context

package gov.census.das.spark.udf

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object sumMapsIntInt extends Aggregator[Map[Int, Int], Map[Int, Int], Map[Int, Int]] {
  override def zero: Map[Int, Int] = Map[Int, Int]()

  override def reduce(buffer: Map[Int, Int], newValue: Map[Int, Int]): Map[Int, Int] = {
    this.sumMapsIntIntElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Map[Int, Int], intermediateValue2: Map[Int, Int]): Map[Int, Int] = {
    this.sumMapsIntIntElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Map[Int, Int]): Map[Int, Int] = {
    reduction
  }

  override def bufferEncoder: Encoder[Map[Int, Int]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Map[Int, Int]] = ExpressionEncoder()

  def sumMapsIntIntElem(map1: Map[Int, Int], map2: Map[Int, Int]): Map[Int, Int] = {
    val result = mutable.Map[Int, Int]()

    // Add all entries from map1 (first converting Array to pairs to map) to result
    map1.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0)
    }

    // Add all entries from map2 (first converting Array to pairs to map) to result
    map2.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0)
    }

    // Convert mutable map to array and return
    result.toMap
  }

  def register(spark: SparkSession, name: String = "sumMapsIntInt"): Unit = {
    spark.udf.register(name, functions.udaf(sumMapsIntInt))
  }

}
