//https://raw.githubusercontent.com/pzhur/bin2longudf/9ba2289c9ef7aa409487bdc904beee7ce67fd931/src/main/scala/gov/census/das/spark/udf/sumMapsIntLong.scala
// Implements Spark UDAF (user defined aggregator function) that can be complied to JAR and called
// from pyspark for summing a column of sparse matrices (in COO format) converted to python dicts {int: long}
// and then spark MapType(IntType(), LongType()) (see unit tests in python folder).
// The use case was calling from pyspark to sum scipy.sparse.coo matrices, but it didn't show performance increase
// vs summing within python in RDD.
// A better way to use spark functions collect_list followed by flatten, and then convert back to COO at the end,
// summing the duplicate elements
// This function can be used though if a column of MapType(IntType(), LongType()) needs to be summed in some other context

package gov.census.das.spark.udf
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object sumMapsIntLong extends Aggregator[Map[Int, Long], Map[Int, Long], Map[Int, Long]] {
  override def zero: Map[Int, Long] = Map[Int, Long]()

  override def reduce(buffer: Map[Int, Long], newValue: Map[Int, Long]): Map[Int, Long] = {
    this.sumMapsIntLongElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Map[Int, Long], intermediateValue2: Map[Int, Long]): Map[Int, Long] = {
    this.sumMapsIntLongElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Map[Int, Long]): Map[Int, Long] = {
    //println(s"Finish called: ${reduction.mkString("Array(", ", ", ")")}")
    reduction
  }

  override def bufferEncoder: Encoder[Map[Int, Long]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Map[Int, Long]] = ExpressionEncoder()

  def sumMapsIntLongElem(map1: Map[Int, Long], map2: Map[Int, Long]): Map[Int, Long] = {
    val result = mutable.Map[Int, Long]()

    // Add all entries from map1 (first converting Array to pairs to map) to result
    map1.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0L).longValue()
    }

    // Add all entries from map2 (first converting Array to pairs to map) to result
    map2.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0L).longValue()
    }

    // Convert mutable map to array and return
    result.toMap
  }

  def register(spark: SparkSession, name: String = "sumMapsIntLong"): Unit = {
    spark.udf.register(name, functions.udaf(sumMapsIntLong))
  }

}
