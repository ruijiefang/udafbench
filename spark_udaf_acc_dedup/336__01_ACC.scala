//https://raw.githubusercontent.com/zhilongWen/spark/6c714f6f35437312bdb1db5785654bfb9a69fd3a/examples/src/main/scala/com/at/spark/core/acc/_01_ACC.scala
// scalastyle:off

package com.at.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @create 2023-10-01 
 */
object _01_ACC {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparConf)

    var rdd = sc.makeRDD(List(1, 2, 3, 4))
    /*
        // reduce : 分区内计算，分区间计算
        //val i: Int = rdd.reduce(_+_)
        //println(i) // i = 10

        var sum = 0
        rdd.foreach(
          num => {
            sum += num
          }
        )
        println("sum = " + sum) // sum = 0

        */

    /*
        // 获取系统累加器
        // Spark默认就提供了简单数据聚合的累加器
        val sumAcc = sc.longAccumulator("sum")

        //sc.doubleAccumulator
        //sc.collectionAccumulator

        rdd.foreach(
          num => {
            // 使用累加器
            sumAcc.add(num)
          }
        )

        // 获取累加器的值
        println(sumAcc.value)

     */

    /*
        // 获取系统累加器
        // Spark默认就提供了简单数据聚合的累加器
        val sumAcc = sc.longAccumulator("sum")

        //sc.doubleAccumulator
        //sc.collectionAccumulator

        val mapRDD = rdd.map(
          num => {
            // 使用累加器
            sumAcc.add(num)
            num
          }
        )

        // 获取累加器的值
        // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
        // 多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
        // 一般情况下，累加器会放置在行动算子进行操作
        mapRDD.collect()
        mapRDD.collect()
        println(sumAcc.value) // 20

      */


    val rddWc = sc.makeRDD(List("hello", "spark", "hello"))

    // 累加器 : WordCount
    // 创建累加器对象
    val wcAcc = new MyAccumulator()
    // 向Spark进行注册
    sc.register(wcAcc, "wordCountAcc")

    rddWc.foreach(
      word => {
        // 数据的累加（使用累加器）
        wcAcc.add(word)
      }
    )

    // 获取累加器累加的结果
    println(wcAcc.value)

    sc.stop()


  }

  /*
    自定义数据累加器：WordCount

    1. 继承AccumulatorV2, 定义泛型
       IN : 累加器输入的数据类型 String
       OUT : 累加器返回的数据类型 mutable.Map[String, Long]

    2. 重写方法（6）
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private var wcMap = mutable.Map[String, Long]()

    // 判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    // Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}

// scalastyle:on