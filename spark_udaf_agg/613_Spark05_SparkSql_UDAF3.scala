//https://raw.githubusercontent.com/smile-j/bigdata_demo/6799871ec7fdcecb23f6e2f5411251938fb33ebf/spark_core/src/main/java/com/demo/bigdata/spark_sql/Spark05_SparkSql_UDAF3.scala
package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn, functions}
object Spark05_SparkSql_UDAF3 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df:DataFrame = spark.read.json("datas/spark_sql/user.json")

    //早期版本中，spark不能在sql中使用强类型UDAF操作
    //SQL & DSL
    //早期的UDAF强类型聚合函数使用DSL语法操作

    val ds:Dataset[User] = df.as[User]

    //将UDAF函数转换为查询的列对象
    val udafColumn: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

    ds.select(udafColumn).show()




    spark.close()

  }


  /**
    * 自定义聚合函数
    * 1.继承 import org.apache.spark.sql.expressions.Aggregator[-IN, BUF, OUT]  定义泛型
    * IN 输入的数据类型 Long
    * BUF 缓冲区的类型
    * OUT 输出的数据类型 Long
    *
    */

  case class User(userName:String,age:Long){

  }
  case class Buff(var total:Long,var count:Long){

  }
  class MyAvgUDAF extends Aggregator[User,Buff,Long]{

    // z&zero 初始值或零值
    //缓冲区的初始化
    override def zero: Buff = {
      Buff(0L,0L)
    }

    //根据输入的数据更新缓冲去的数据
    override def reduce(buff: Buff, in: User): Buff = {
      buff.total = buff.total+in.age
      buff.count = buff.count +1
      buff
    }

    //合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total+b2.total
      b1.count = b1.count+b2.count
      b1
    }

    //计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total/reduction.count
    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
