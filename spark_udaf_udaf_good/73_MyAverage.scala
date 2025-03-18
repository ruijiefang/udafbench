//https://raw.githubusercontent.com/SCUTJoeham/Spark_Bigdata_Analysis_in_Action/af5c8cc932a19b2b4e0cb110b5c7b600f48348c2/chapter5/5.5.2/MyAverage.scala
package spark.demo

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

/**
  * 用户自定义聚合函数类，实现求平均值
  */
class MyAverage extends UserDefinedAggregateFunction {
   // 聚合函数输入参数的类型，运行时会将需要聚合的每一个值输入到聚合函数中
   //inputColumn为输入的列名，不做特殊要求，相当于一个列名占位符
   override def inputSchema: StructType = StructType(List(
      StructField("inputColumn", LongType)
   ))
   // 定义存储聚合运算产生的中间数据的Schema
   // sum和count不做特殊要求，为自定义名称
   override def bufferSchema: StructType = StructType(List(
      StructField("sum", LongType),//参与聚合的数据总和
      StructField("count", LongType)//参与聚合的数据数量
   ))
   override def dataType: DataType = DoubleType
   // 针对给定的同一组输入，聚合函数是否返回相同的结果,通常为true
   override def deterministic: Boolean = true
   // 初始化聚合运算的中间结果，中间结果存储于buffer中，buffer是一个Row类型
   override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0)=0L //与bufferSchema中的第一个字段（sum）对应，即sum的初始值
      buffer(1)=0L //与bufferSchema中的第二个字段（count）对应，即count的初始值
   }
   // 由于参与聚合的数据会依次输入聚合函数，每当向聚合函数输入新的数据时，
   // 都会调用该函数更新聚合中间结果
   override def update(buffer: MutableAggregationBuffer,input: Row):Unit ={
      if (!input.isNullAt(0)) {
         buffer(0)= buffer.getLong(0)+input.getLong(0)//更新参与聚合的数据总和
         buffer(1) = buffer.getLong(1) + 1//更新参与聚合的数据数量
      }
   }
   // 合并多个分区的buffer中间结果（分布式计算，参与聚合的数据存储于多个分区，
   // 每个分区都会产生buffer中间结果）
   override def merge(buffer1: MutableAggregationBuffer, buffer2: Row):Unit={
      buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)//合并参与聚合的数据总和
      buffer1(1)= buffer1.getLong(1)+buffer2.getLong(1)//合并参与聚合的数据数量
   }
   // 计算最终结果，数据总和/数据数量=平均值
   override def evaluate(buffer: Row): Double =
      buffer.getLong(0).toDouble / buffer.getLong(1)
}

/**
  * 测试自定义聚合函数
  */
object MyAverage{
   def main(args: Array[String]): Unit = {
      //创建或得到SparkSession
      val spark = SparkSession
        .builder()
        .appName("Spark SQL UDAF example")
        .master("local[*]")
        .getOrCreate()

      // 注册自定义聚合函数
      spark.udf.register("MyAverage",new MyAverage)
      // 读取JSON数据
      val df = spark.read.json("D:/test/employees.json")
      //创建临时视图
      df.createOrReplaceTempView("employees")
      df.show()
      // +-------+------+
      // |   name|salary|
      // +-------+------+
      // |Michael|  3000|
      // |   Andy|  4500|
      // | Justin|  3500|
      // |  Berta|  4000|
      // +-------+------+

      // 调用聚合函数进行查询
      val result = spark.sql(
         "SELECT myAverage(salary) as average_salary FROM employees"
      )
      //显示查询结果
      result.show()
      // +--------------+
      // |average_salary|
      // +--------------+
      // |        3750.0|
      // +--------------+
      spark.stop()
   }
}
