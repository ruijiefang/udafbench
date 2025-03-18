//https://raw.githubusercontent.com/smile-j/bigdata_demo/6799871ec7fdcecb23f6e2f5411251938fb33ebf/spark_core/src/main/java/com/demo/bigdata/spark_sql/Spark08_SparkSql_Test2.scala
package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Spark08_SparkSql_Test2 {

  def main(args: Array[String]): Unit = {

//    Class.forName("org.apache.spark.sql.hive.HiveSessionStateBuilder")

    //jdbc:hive2://hadoop102:10000
    //user:uhadoop
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("user test_demo")

    spark.udf.register("cityRemark",functions.udaf(new CityRemarkUDAF()))
    //基本查询
    spark.sql(
      """
        |		select a.*,p.product_name,c.city_name
        |		from user_visit_action a
        |		join product_info p on p.product_id=a.click_product_id
        |		join city_info c on c.city_id = a.city_id
        |		where a.click_product_id>-1
      """.stripMargin).createOrReplaceTempView("t1")

    //根据区域，商品进行数据聚合
    spark.sql(
      """
        |select area
        |,product_name
        |,count(*) clickCnt
        |,cityRemark(city_name) as city_remark
        |from t1
        |group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")

    //分区内对点击数量进行排行
    spark.sql(
      """
        |select *,rank() over(partition by area order by clickCnt desc ) as rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    //取前三名
    spark.sql(
      """
        | select *
        | from t3
        |  where t3.rank<=3
      """.stripMargin).show(false)

    spark.close()
  }

  case class Buffer(var total:Long,var cityMap:mutable.Map[String,Long]){

  }
  class CityRemarkUDAF extends  Aggregator[String,Buffer,String]{
    override def zero: Buffer = {
      Buffer(0,mutable.Map[String,Long]())
    }

    override def reduce(buffer: Buffer, city: String): Buffer = {
      buffer.total+=1
      val newCount = buffer.cityMap.getOrElse(city,0L)+1
      buffer.cityMap.update(city,newCount)
      buffer
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total+=b2.total

      val map1 = b1.cityMap
      val map2 = b2.cityMap

      b1.cityMap = map1.foldLeft(map2){
        case (map,(city,cnt))=>{
          val newCount = map.getOrElse(city,0L)+1
          map.update(city,newCount)
          map
        }
      }
      b1

    }

    override def finish(buffer: Buffer): String = {
      var remarkList = ListBuffer[String]()

      var totalCnt = buffer.total
      val cityMap = buffer.cityMap
      val cityCntList = cityMap.toList.sortWith((left, right) => {
        left._2 > right._2
      }).take(2)


      val hasMore = cityMap.size > 2
      var rsum = 0L

      cityCntList.foreach{
        case (city,cnt)=>{
          val r = cnt*100 /totalCnt
          remarkList.append(s"${city} ${r}%")
          rsum += r
        }
      }
      if(hasMore){
        remarkList.append(s"其他 ${100-rsum}")
      }

      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
