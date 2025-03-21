//https://raw.githubusercontent.com/zixuedanxin/bigdata_learn/967411b94f798de7e2961f3a84a112e31e21f206/spark-scala-learn/src/main/scala/com/bigdata/sql/n_02_spark_dataset/user_defined_type_aggregation/n_01_udta_MyAverage/Run.scala
package com.bigdata.sql.n_02_spark_dataset.user_defined_type_aggregation.n_01_udta_MyAverage

import com.bigdata.standalone.base.BaseSparkSession
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

object Run extends BaseSparkSession{

  case class Employee(name: String, salary: Long)
  case class Average(var sum: Long, var count: Long)

  /**
    * 自定义聚合类
    * Employee : DataSet每条数据的类型
    * Average : 结果类型，进行DataSet中元素之间计算时，结果保存到这个计算结果类中
    * Double: 最终的计算结果类型，返回值,计算完所有的数据集元素后，调用finish()数得到结果数据
    */
  object MyAverage extends Aggregator[Employee,Average,Double]{

    //如果DataSet没数据，返回什么样的结果
    def zero:Average = Average(0L,0L)

    def reduce(b: Average, a: Employee): Average ={
      b.sum += a.salary
      b.count += 1
      b
    }

    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[Average] = Encoders.product
    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble


  }

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)

    import spark.implicits._
    val ds = spark.read.json("src/main/resource/data/json/employees.json").as[Employee]
    ds.show()
//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//    |  30|   Andy|
//    |  19| Justin|
//    +----+-------+

    val averageColumn = MyAverage.toColumn.name("平均值")
    ds.select(averageColumn).show()





    spark.stop()
  }

}
