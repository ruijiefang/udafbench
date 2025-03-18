//https://raw.githubusercontent.com/thisishotcenplotz/study-notes/925cbcfd4a268d05472079aa3d3820b887d30fbf/bigdata/spark-tutorial/spark-core/src/main/scala/com/bigdata/spark/sql/SparkSQL03_UDAF.scala
package com.bigdata.spark.sql

import org.apache.spark.{Aggregator, SparkConf}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, expressions, functions}


object SparkSQL03_UDAF {
  def main(args: Array[String]): Unit = {

    //TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark sql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //TODO 执行逻辑操作
    val frame = spark.read.json("data/usr.json")
    val user = frame.createOrReplaceTempView("user")
    spark.udf.register("avgAge", functions.udaf(new MyAvgUDAF))
    spark.sql("select username,age,avgAge(age) over() as avg_age from user").show()






    //TODO 关闭环境
    spark.close()
  }

  // 定义泛型
  // IN: 输入的数据类型
  // BUF: 缓冲区的数据类型
  // OUT: 输出的数据类型
  //
  case class Buff(var total:Long,var count:Long)
  import org.apache.spark.sql.expressions.Aggregator
  class MyAvgUDAF extends Aggregator[Long,Buff,Long]{
    //缓冲区初始化
    override def zero: Buff = {
      Buff(0L,0L)
    }

    // 根据输入的数据来更新缓冲区的数据
    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total + a
      b.count = b.count + 1
      b
    }

    // 合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: Buff): Long = {
      reduction.total/reduction.count
    }

    // 缓冲区序列化编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }



}
