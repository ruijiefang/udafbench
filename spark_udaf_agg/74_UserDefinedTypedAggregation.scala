//https://raw.githubusercontent.com/DoctorQ/learningspark/3b4ca3aee5595f6e09922e1d22271ddbe57d0964/src/main/scala/com/doctorq/spark/sql/UserDefinedTypedAggregation.scala
package com.doctorq.spark.sql

import com.doctorq.spark.ml.SparkObject
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


object UserDefinedTypedAggregation extends SparkObject {

  case class Employee(name: String, salary: Long)

  case class Average(var sum: Long, var count: Long)


  object MyAverage extends Aggregator[Employee, Average, Double] {
    override def zero: Average = Average(0L, 0L)

    override def reduce(b: Average, a: Employee): Average = {
      b.sum += a.salary
      b.count += 1
      b
    }

    override def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    override def bufferEncoder: Encoder[Average] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }


  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val ds = spark.read.json("src/main/resources/employees.json").as[Employee]
    ds.show()
    val averageSalary = MyAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()
  }


}
