//https://raw.githubusercontent.com/yuriFish/sparkDemo/29c1bccdd476eba2c88d75b7f241d87168d3714c/src/main/scala/com/atguigu/bigdata/spark/core/exam/helper/HotCategoryAccumulator.scala
package com.atguigu.bigdata.spark.core.exam.helper

import com.atguigu.bigdata.spark.core.exam.bean.HotCategory
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * 热门品类累加器
 * 1. 继承AccumulatorV2，定义泛型[In, Out]
 *    In: (品类, 行为类型)
 *    Out: Map[品类, HotCategory]
 * 2. 重写方法(6)
 */
class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]{

  val hotCategoryMap = mutable.Map[String, HotCategory]()

  override def isZero: Boolean = hotCategoryMap.isEmpty

  override def copy() = new HotCategoryAccumulator

  override def reset(): Unit = hotCategoryMap.clear()

  override def add(v: (String, String)): Unit = {
    val cid = v._1
    val actionType = v._2

    val hotCategory: HotCategory = hotCategoryMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))

    actionType match {
      case "click" => hotCategory.clickCount += 1
      case "order" => hotCategory.orderCount += 1
      case "pay"   => hotCategory.payCount += 1
      case _       =>
    }

    hotCategoryMap(cid) = hotCategory

  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
    other.value.foreach{
      case (cid, hotCategory) => {
        val hc: HotCategory = hotCategoryMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
        hc.clickCount += hotCategory.clickCount
        hc.orderCount += hotCategory.orderCount
        hc.payCount += hotCategory.payCount

        hotCategoryMap(cid) = hc
      }
    }
  }

  override def value = hotCategoryMap
}
