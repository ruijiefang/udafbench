//https://raw.githubusercontent.com/wxguo-shou/bigdata/e1a71297b6dd201587ec9071f09d2f30a4f480e4/spark/atguigu-classes/spark-core/src/main/scala/com/atguigu/spark/core/req/Spark04_Req1.scala
package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author name 婉然从物
 * @create 2023-11-12 9:16
 */
object Spark04_Req1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")

    val sc = new SparkContext(sparkConf)

    // Q1: actionRDD 重复使用
    // Q2: cogroup可能性能较低

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("atguigu-classes/datas/user_visit_action.txt")

    val acc = new MyAccumulator
    sc.register(acc, "horCategory")

//    actionRDD.foreach(
//      action => {
//        val datas: Array[String] = action.split("_")
//        datas match {
//          case _ if datas(6) != "-1" => acc.add(datas(6), "click")
//          case _ if datas(8) != "null" => {
//            val ids: Array[String] = datas(8).split(",")
//            ids.foreach(id => acc.add(id, "order"))
//          }
//          case _ if datas(10) != "null" => {
//            val ids: Array[String] = datas(10).split(",")
//            ids.foreach(id => acc.add(id, "pay"))
//          }
//        }
//
//      }
//    )

    actionRDD.foreach(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          acc.add((datas(6), "click"))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(id => acc.add((id, "order")))
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.foreach(id => acc.add((id, "pay")))
        }
      }
    )


    val accVal: mutable.Map[String, Hotcategory] = acc.value
    val hotcategories: mutable.Iterable[Hotcategory] = accVal.map(_._2)

    val sort: List[Hotcategory] = hotcategories.toList.sortWith { (left, right) =>
      if (left.clickCnt != right.clickCnt) {
        left.clickCnt > right.clickCnt
      } else if (left.orderCnt != right.orderCnt) {
        left.orderCnt > right.orderCnt
      } else {
        left.payCnt > right.payCnt
      }
    }


    sort.take(10).foreach(println)

    sc.stop()

  }

  case class Hotcategory(cid:String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  /**
   * 自定义累加器
   * 1. 继承AccumulatorV2， 定义泛型
   *    IN: (品类ID, 行为类型)
   *    OUT: mutable.Map[String, Hotcategory]
   * 2. 重写方法 (6)
   */

  class MyAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, Hotcategory]]{

    private val hcMap: mutable.Map[String, Hotcategory] = mutable.Map[String, Hotcategory]()
    
    override def isZero: Boolean = hcMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, Hotcategory]] = {
      new MyAccumulator
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category: Hotcategory = hcMap.getOrElse(cid, Hotcategory(cid, 0, 0, 0))
//      actionType match {
//        case  actionType == "click" => category.clickCnt += 1
//        case  actionType == "order" => category.orderCnt += 1
//        case  actionType == "pay" => category.payCnt += 1
//      }

      if (actionType == "click"){
        category.clickCnt += 1
      } else if (actionType == "order"){
        category.orderCnt += 1
      } else if (actionType == "pay"){
        category.payCnt += 1
      }

      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, Hotcategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value

      map2.foreach{
        case (cid, hc) => {
          val category: Hotcategory = map1.getOrElse(cid, Hotcategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, Hotcategory] = hcMap
  }

}
