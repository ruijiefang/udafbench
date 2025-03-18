//https://raw.githubusercontent.com/thisishotcenplotz/study-notes/925cbcfd4a268d05472079aa3d3820b887d30fbf/bigdata/spark-tutorial/spark-core/src/main/scala/com/bigdata/spark/core/RddExercise/UsrAction04_HotCategoryTop10.scala
package com.bigdata.spark.core.RddExercise

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object UsrAction04_HotCategoryTop10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Top 10")
    val spark = new SparkContext(conf)

    // Q: 存在大量的shuffle操作（reduceByKey）
    // reduceByKey 聚合算子，spark会提供优化，缓存


    // 1. 读取原始日志数据
    val rdd = spark.textFile("data/fake_user_visit_action.txt")
    val accumulator = new HotCategoryAccumulator()
    spark.register(accumulator,"HotCategory")
    rdd.foreach(
      action => {

        val data = action.split("_")

        if (data(6) != "null") {
          accumulator.add(data(6),"click")
        }
        else if (data(8) != "null") {
          val ids = data(8).split("-")
          ids.foreach(
            id => {
              accumulator.add((id,"order"))
            }
          )

        }
        else if (data(10) != "null") {
          val ids = data(10).split("-")
          ids.foreach(
            id => {
              accumulator.add((id, "paid"))
            }
          )
        }
      }
    )

    val accVal = accumulator.value
    val categories = accVal.map(_._2)
    val sortResult = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) true
        else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) true
          else if (left.orderCnt == right.orderCnt) {
            left.paidCnt > right.paidCnt
          }
          else false
        }
        else false
      }
    )
    sortResult.take(10).foreach(println)



    spark.stop()
  }


  // 自定义累加器
  // 定义泛型
  // IN: (品类ID，行为类型)
  // OUT：（品类ID，mutable.Map[String,Hotcategory]）

  case class HotCategory(id:String,var clickCnt:Int,var orderCnt:Int,var paidCnt:Int)
  class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{
    private val hcMap = mutable.Map[String,HotCategory]()
    override def isZero: Boolean = hcMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulator()

    override def reset(): Unit = hcMap.clear()

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if(actionType == "click"){
        category.clickCnt += 1
      }
      else if (actionType == "order"){
        category.orderCnt += 1
      }
      else if (actionType == "paid"){
        category.paidCnt += 1
      }
      hcMap.update(cid,category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value
      map2.foreach {
        case (cid, hc) => {
          val category = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.paidCnt += hc.paidCnt
          map1.update(cid,category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }

}
