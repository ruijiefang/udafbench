//https://raw.githubusercontent.com/smile-j/bigdata_demo/6799871ec7fdcecb23f6e2f5411251938fb33ebf/spark_core/src/main/java/com/demo/bigdata/spark_core/rdd/req/Spar04_HotCategoryTop10.scala
package com.demo.bigdata.spark_core.rdd.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spar04_HotCategoryTop10 {

  def main(args: Array[String]): Unit = {

    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    //q1:大量的shuffle(reduceByKey)
    //reduceByKey 聚合算子，spark会提供优化，缓存

    //1.读取原始日志数据
    val actionRdd = sc.textFile("datas/user_visit_action.txt")

    val acc:HotCategoryAccumulator= new HotCategoryAccumulator
    sc.register(acc)

    actionRdd.foreach(action => {
      val datas:Array[String] = action.split("_")
      //点击场景
      if (datas(6) != "-1") {
        acc.add((datas(6), "click"))
        //下单
      }else if (datas(8) != "null") {
        val ids = datas(8).split(",")
        ids.foreach(
          id =>{
            acc.add((id, "order"))
          }
        )
      } else if (datas(10) != "null") {
        val ids = datas(10).split(",")
        ids.foreach(
          id =>{
            acc.add((id, "pay"))
          }
        )
      }

    })
    val accValue:mutable.Map[String,HotCategory] = acc.value
    val category:mutable.Iterable[HotCategory] = accValue.map({
      case (cid, hc) => {
        hc
      }
    }
    )
    val result = category.toList.sortWith(
      (left,right)=>{
        if(left.clickCnt>right.clickCnt){
          true
        }else if ((left.clickCnt==right.clickCnt)){
          if(left.orderCnt>right.orderCnt){
            true
          }else if(left.orderCnt==right.orderCnt){
            if(left.payCnt>right.payCnt){
              true
            }else{
              false
            }
          }else{
            false
          }
        }else{
          false
        }
      }
    )
      .take(10)
    //6.结果打印
    result.foreach(println)

    sc.stop()

  }

  case class HotCategory(cid:String,var clickCnt:Int,var orderCnt:Int,var payCnt:Int){

  }
  /**
    * 自定义累加器
    * 1.定义泛型：
    *     in:(品类id,行为类型)
    *     out:map[String,HotCategory]
    */
  class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{

    private val map = mutable.Map[String,HotCategory]()

    override def isZero: Boolean = {
      map.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      this.map.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category = map.getOrElse(cid,HotCategory(cid,0,0,0))
      if(actionType == "click"){
          category.clickCnt  +=  1
      }else if(actionType == "order"){
        category.orderCnt += 1
      }else if (actionType == "pay"){
        category.payCnt += 1
      }
      map.update(cid,category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.map
      val map2 = other.value

      map2.foreach{
        case (cid,hc)=>{
          val category = map1.getOrElse(cid,HotCategory(cid,0,0,0))
          category.clickCnt+= hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid,category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = map
  }

}
