//https://raw.githubusercontent.com/AngerWind/example4all/1ff3c59fa01aa6016cdbd400432507867b57ad63/spark-scala/src/main/scala/com/tiger/spark/_2_rdd/practice/HotCategoryTopN4.scala
package com.tiger.spark._2_rdd.practice

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.collection.mutable


class HotCategoryTopN4 extends Serializable {

  /**
   * 不使用累加器, 直接聚合
   */
  @Test
  def practice(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aaa").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    val result: mutable.Map[String, (Int, Int, Int)] = context.textFile("input/user_visit_action.csv")
      .aggregate(mutable.Map[String, (Int, Int, Int)]())((map, str) => {
        val strings: Array[String] = str.split(",")
        // 点击数据
        if (strings(6) != "-1" && strings(7) != "-1") {
          val key = strings(6)
          val count: (Int, Int, Int) = map.getOrElse(key, (0, 0, 0))
          map.update(key, (count._1 + 1, count._2, count._3))
        }
        // 下单数据
        else if (strings(8) != "" && strings(9) != "") {
          strings(8).split("-").foreach(key => {
            val count: (Int, Int, Int) = map.getOrElse(key, (0, 0, 0))
            map.update(key, (count._1, count._2 + 1, count._3))
          })
        }
        // 支付数据
        else if (strings(10) != "" && strings(11) != "") {
          strings(10).split("-").foreach(key => {
            val count: (Int, Int, Int) = map.getOrElse(key, (0, 0, 0))
            map.update(key, (count._1, count._2, count._3 + 1))
          })
        }
        map
      }, (map, other) => {
        other.foreach({
          case (key, count2) =>
            val count1: (Int, Int, Int) = map.getOrElse(key, (0, 0, 0))
            map.update(key, (count1._1 + count2._1, count1._2 + count2._2, count1._3 + count2._3))
        })
        map
      })

    result.toArray.sortBy(_._2)(Ordering.Tuple3[Int, Int, Int].reverse).take(10).foreach(println)
    context.stop()
  }
}
