//https://raw.githubusercontent.com/pzhur/bin2longudf/9ba2289c9ef7aa409487bdc904beee7ce67fd931/src/main/scala/gov/census/das/spark/udf/sumMapsAsArray.scala
// Implements Spark UDAF (user defined aggregator function) that can be complied to JAR and called
// from pyspark for summing a column of sparse matrices (in COO format) converted to interleaved arrays of Ints
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



object sumMapsAsArray extends Aggregator[Array[Int], Array[Int], Array[Int]] {
  override def zero: Array[Int] = Array[Int]()

  override def reduce(buffer: Array[Int], newValue: Array[Int]): Array[Int] = {
    this.sumMapsAsArrayElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Array[Int], intermediateValue2: Array[Int]): Array[Int] = {
    this.sumMapsAsArrayElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Array[Int]): Array[Int] = {
    reduction
  }

  override def bufferEncoder: Encoder[Array[Int]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Array[Int]] = ExpressionEncoder()

  def sumMapsAsArrayElem(map1: Array[Int], map2: Array[Int]): Array[Int] = {
    val result = mutable.Map[Int, Int]()
    require(map1.length % 2 == 0, "Array length must be even for key-value pairs")
    require(map2.length % 2 == 0, "Array length must be even for key-value pairs")

    // Add all entries from map1 (first converting Array to pairs to map) to result
    map1.grouped(2).map {
      case Array(key: Int, value: Int) => (key, value)
    }.toMap.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0)
    }

    // Add all entries from map2 (first converting Array to pairs to map) to result
    map2.grouped(2).map {
      case Array(key: Int, value: Int) => (key, value)
    }.toMap.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0)
    }

    // Convert mutable map to array and return
    result.toArray.flatMap { case (key, value) => Array(key, value) }
  }

  def register(spark: SparkSession, name: String = "sumMapsAsArray"): Unit = {
    spark.udf.register(name, functions.udaf(sumMapsAsArray))
  }

}
