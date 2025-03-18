//https://raw.githubusercontent.com/LJunTing/hadoop-sh/16f2f744729bfa9e8c530b1c6ff28240c9e99447/spark/src/main/scala/test/sql/SparkSQL05_UDAF_class.scala
package test.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}

object SparkSQL05_UDAF_class {

  def main(args: Array[String]): Unit = {


    //2.3  spark  创建方式
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("sqlTest").getOrCreate()
    //进行转换之前需要引入隐式转换规则
    import spark.implicits._


    val testDf: DataFrame = spark.read.json("in/test.json")

    //自定义聚合儿函数
    val udaf = new MyAvgClassFun
    //聚合函数转换为列
    val avgcolumn: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgage")
    // df 转换为 ds
    val userds: Dataset[UserBean] = testDf.as[UserBean]
    //ds 用dsl  风格查询列
    userds.select(avgcolumn).show()

    spark.stop()
  }

}

case class UserBean(name: String, age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)


//声明用户自定义聚合函数
/**
  * 继承aggregator   这个包下的: org.apache.spark.sql.expressions.Aggregator
  * 实现方法
  */

class MyAvgClassFun extends Aggregator[UserBean, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  /**
    * 聚合数据
    */
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1

    b
  }

  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count

    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}