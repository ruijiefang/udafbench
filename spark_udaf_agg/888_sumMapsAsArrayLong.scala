//https://raw.githubusercontent.com/pzhur/bin2longudf/9ba2289c9ef7aa409487bdc904beee7ce67fd931/src/main/scala/gov/census/das/spark/udf/sumMapsAsArrayLong.scala
// Implements Spark UDAF (user defined aggregator function) that can be complied to JAR and called
// from pyspark for summing a column of sparse matrices (in COO format) converted to interleaved arrays of Longs
// as [ind0, val0, ind1, val1...].
// The use case was calling from pyspark to sum scipy.sparse.coo matrices, but it didn't show performance increase
// vs summing within python in RDD.
// A better way to use spark functions collect_list followed by flatten, and then convert back to COO at the end,
// summing the duplicate elements
package gov.census.das.spark.udf

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}


object sumMapsAsArrayLong extends Aggregator[Array[Long], Array[Long], Array[Long]] {
  override def zero: Array[Long] = Array[Long]()

  override def reduce(buffer: Array[Long], newValue: Array[Long]): Array[Long] = {
    this.sumMapsAsArrayElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Array[Long], intermediateValue2: Array[Long]): Array[Long] = {
    this.sumMapsAsArrayElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Array[Long]): Array[Long] = {
    reduction
  }

  override def bufferEncoder: Encoder[Array[Long]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Array[Long]] = ExpressionEncoder()

  def sumMapsAsArrayElem(map1: Array[Long], map2: Array[Long]): Array[Long] = {
    val result = mutable.Map[Long, Long]()
    require(map1.length % 2 == 0, "Array length must be even for key-value pairs")
    require(map2.length % 2 == 0, "Array length must be even for key-value pairs")

    // Add all entries from map1 (first converting Array to pairs to map) to result
    map1.grouped(2).map {
      case Array(key: Long, value: Long) => (key, value)
    }.toMap.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0L).longValue()
    }

    // Add all entries from map2 (first converting Array to pairs to map) to result
    map2.grouped(2).map {
      case Array(key: Long, value: Long) => (key, value)
    }.toMap.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0L).longValue()
    }

    // Convert mutable map to array and return
    result.toArray.flatMap { case (key, value) => Array(key, value) }
  }

  def register(spark: SparkSession, name: String = "sumMapsAsArrayLong"): Unit = {
    spark.udf.register(name, functions.udaf(sumMapsAsArrayLong))
  }

}
