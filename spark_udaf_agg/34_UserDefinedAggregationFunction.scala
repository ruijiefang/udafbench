//https://raw.githubusercontent.com/iahsanujunda/maxcompute-spark/51a3467b78b1bf130167ce16ab79154aa32f4d2c/src/main/scala/id/dana/spark/odps/sql/UserDefinedAggregationFunction.scala
package id.dana.spark.odps.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions

object UserDefinedAggregationFunction {

  case class Average(var sum: Long, var count: Long)

  object MyAverage extends Aggregator[Long, Average, Double] {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    def zero: Average = Average(0L, 0L)
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: Average, data: Long): Average = {
      buffer.sum += data
      buffer.count += 1
      buffer
    }
    // Merge two intermediate values
    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }
    // Transform the output of the reduction
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[Average] = Encoders.product
    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL user-defined DataFrames aggregation example")
      .getOrCreate()
    val tableName = "ods_people"

    // Register the function to access it
    spark.udf.register("myAverage", functions.udf(MyAverage))

    val sql_df = spark.sql(s"select * from $tableName")
    sql_df.printSchema()

    sql_df.createOrReplaceTempView("employees")
    sql_df.show()

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()

    spark.stop()
  }
}
