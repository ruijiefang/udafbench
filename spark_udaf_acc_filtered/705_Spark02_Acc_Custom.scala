//https://raw.githubusercontent.com/xuyq0618/bigdata_spark01/4cf04a97056cacf093d92cf1e33fcce00fd90cb4/spark-core/src/main/java/com/alibaba/bigdata/spark/core/Acc/Spark02_Acc_Custom.scala
package com.alibaba.bigdata.spark.core.Acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_Acc_Custom {
  def main(args: Array[String]): Unit = {
    //1、建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //2、累加器
    val rdd = sc.makeRDD(List("hello","spark","hello","scala"))
    //创建累加器对象
    val wcAcc = new MyAccumulator()
    //向spark进行注册
    sc.register(wcAcc,"wordCountAcc")

    rdd.foreach(
      word => {
        wcAcc.add(word)
      }
    )

    //获取累加器的值
    println(wcAcc.value)

    //    3、关闭连接
    sc.stop()
  }

  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]] {
    private var wcMap = mutable.Map[String,Long]()

    //判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    //获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L)+1
      wcMap.update(word,newCnt)
    }

    //Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach{
        case(word,count) => {
          val newCount = map1.getOrElse(word,0L)+count
          map1.update(word,newCount)
        }
      }
    }

    //累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}
