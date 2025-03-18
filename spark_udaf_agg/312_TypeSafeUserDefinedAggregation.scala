//https://raw.githubusercontent.com/ghazi-naceur/spark-fundamentals/991400ee097cd93baf4218a2157a05a0c7bdc113/src/main/scala/official/doc/examples/sql/c1_getting_started/TypeSafeUserDefinedAggregation.scala
package official.doc.examples.sql.c1_getting_started

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, TypedColumn}

case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)

object TypeSafeUserDefinedAggregation extends Aggregator[Employee, Average, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
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

object Average {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .getOrCreate()

    import spark.implicits._
    val ds = spark.read.json("src/main/resources/official.doc/employees.json").as[Employee]
    ds.show()

    // Convert the function to a `TypedColumn` and give it a name
    val averageSalary: TypedColumn[Employee, Double] = TypeSafeUserDefinedAggregation.toColumn.name("average_salary")
    val result: Dataset[Double] = ds.select(averageSalary)
    result.show()
  }
}