//https://raw.githubusercontent.com/wxguo-shou/bigdata/e1a71297b6dd201587ec9071f09d2f30a4f480e4/spark/atguigu-classes/spark-core/src/main/scala/com/atguigu/spark/sql/Spark03_UDAF2.scala
package com.atguigu.spark.sql

import com.atguigu.spark.sql.Spark03_UDAF2.MyAvgUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
 * @author name 婉然从物
 * @create 2023-11-12 20:26
 */
object Spark03_UDAF2 {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSql的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // TODO 执行逻辑操作
    val df: DataFrame = spark.read.json("atguigu-classes/datas/user.json")

    // 早期版本中， spark不能在sql中使用强类型UDAF操作
    // SQL & DSL
    // 早期的UDAF强类型聚合函数使用DSL语法
    val ds: Dataset[User] = df.as[User]

    val udafCol: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

    ds.select(udafCol).show()

    // TODO 关闭环境
    spark.close()
  }

  /*
  自定义聚合函数类： 计算年龄的平均值
  1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
    IN: 输入数据的类型  User
    BUF: 缓冲区的数据类型  Buff
    OUT: 输出数据的类型  Long

  2. 重写方法（6）
   */

  case class User(username: String, age: Long)
  case class Buff(var total: Long, var count: Long)
  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
    // z & zero : 初始值或者0值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(b: Buff, a: User): Buff = {
      b.total = b.total + a.age
      b.count += 1
      b
    }

    // 合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total += b2.total
      b1.count += b2.count
      b1
    }

    // 计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
