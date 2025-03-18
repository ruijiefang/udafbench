//https://raw.githubusercontent.com/icefery/demo.icefery.xyz/3f64d732375a34f1c226fab1a067bb37af1be3d8/root/bigdata/spark/__demo__/spark-demo/src/main/scala/org/example/tutorial/sql/Requirement1.scala
package org.example.tutorial.sql

import org.apache.spark.sql.{functions, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Requirement1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Requirement1").master("local[*]").enableHiveSupport().getOrCreate()

    // 注册函数
    spark.udf.register("city_remark", functions.udaf(CityRemark))

    // 建表
    // createTable(spark)

    // 导入数据
    // loadData(spark)

    // 查询 Top3
    showTop3(spark)
  }

  private def showTop3(spark: SparkSession): Unit = {
    spark.sql(
      """
        |select *
        |from (
        |  select *, rank() over(partition by area order by click_count desc) as rank
        |  from (
        |    select
        |      area,
        |      product_name,
        |      count(*) as click_count,
        |      city_remark(city_name) as city_remark
        |    from (
        |      select pi.product_name, ci.area, ci.city_name
        |      from user_visit_action uva
        |      join product_info pi on pi.product_id = uva.click_product_id
        |      join city_info ci on ci.city_id = uva.city_id
        |      where uva.click_product_id > -1
        |    ) t1
        |    group by product_name, area
        |  ) t2
        |) t3
        |where rank <= 3
        |""".stripMargin
    ).show()
  }

  private def createTable(spark: SparkSession): Unit = {
    spark.sql(
      """
        |create table if not exists `user_visit_action`(
        |  `date`               string,
        |  `user_id`            bigint,
        |  `session_id`         string,
        |  `page_id`            bigint,
        |  `action_time`        string,
        |  `search_keyword`     string,
        |  `click_category_id`  bigint,
        |  `click_product_id`   bigint,
        |  `order_category_ids` string,
        |  `order_product_ids`  string,
        |  `pay_category_ids`   string,
        |  `pay_product_ids`    string,
        |  `city_id`            bigint
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    ).show()
    spark.sql(
      """
        |create table if not exists `product_info`(
        |  `product_id`   bigint,
        |  `product_name` string,
        |  `extend_info`  string
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    ).show()
    spark.sql(
      """
        |create table if not exists `city_info`(
        |  `city_id`   bigint,
        |  `city_name` string,
        |  `area`      string
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    ).show()
  }

  private def loadData(spark: SparkSession): Unit = {
    spark.sql("load data local inpath 'input/user_visit_action.txt' overwrite into table user_visit_action;").show()
    spark.sql("load data local inpath 'input/product_info.txt' overwrite into table product_info;").show()
    spark.sql("load data local inpath 'input/city_info.txt' overwrite into table city_info;").show()
  }

  case class CityRemark(var total: Long, var cityMap: mutable.Map[String, Long])

  object CityRemark extends Aggregator[String, CityRemark, String] {
    override def zero: CityRemark = CityRemark(0L, mutable.Map[String, Long]())

    override def reduce(buffer: CityRemark, city: String): CityRemark = {
      buffer.total += 1
      buffer.cityMap.update(city, buffer.cityMap.getOrElse(city, 0L) + 1)
      buffer
    }

    override def merge(buffer1: CityRemark, buffer2: CityRemark): CityRemark = {
      buffer1.total += buffer2.total
      buffer2.cityMap.foreach(it => it match {
        case (city, count) => buffer1.cityMap.update(city, buffer1.cityMap.getOrElse(city, 0L) + count)
      })
      buffer1
    }

    override def finish(reduction: CityRemark): String = {
      val remarkList = ListBuffer[String]()
      var sum = 0L

      reduction
        .cityMap
        .toList
        .sortWith((a, b) => a._2 > b._2)
        .take(2)
        .foreach(it => it match {
          case (city, cnt) => {
            val percentage = cnt * 100 / reduction.total
            remarkList.append(s"${city} ${percentage}%")
            sum += percentage
          }
        })

      if (reduction.cityMap.size > 2) {
        remarkList.append(s"其他 ${100 - sum}%")
      }

      remarkList.mkString(", ")
    }

    override def bufferEncoder: Encoder[CityRemark] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
