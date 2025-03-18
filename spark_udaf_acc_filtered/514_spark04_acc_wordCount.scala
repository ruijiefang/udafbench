//https://raw.githubusercontent.com/thisishotcenplotz/study-notes/925cbcfd4a268d05472079aa3d3820b887d30fbf/bigdata/spark-tutorial/spark-core/src/main/scala/com/bigdata/spark/core/acc/spark04_acc_wordCount.scala
package com.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object spark04_acc_wordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc word count")
    val spark = new SparkContext(sparkConf)

    val rdd = spark.parallelize(List("hello spark", "hello hadoop", "hello flink", "hello scala", "hello iceberg"))

    // 自定义累加器: WordCount
    // 创建累加器对象
    val wcAcc = new MyWordCountAccumulator
    // 向spark进行注册
    spark.register(wcAcc, "wcAcc")


    rdd.flatMap(_.split(" "))
      .foreach(word => {
        wcAcc.add(word)
      })
    println(wcAcc.value.toList.sortWith((t1,t2) => t1._2 > t2._2).mkString(","))
  }

  // 继承AccumulatorV2 定义泛型
  // IN: 累加器输入的数据类型
  // OUT: 累加器返回的数据类型

  class MyWordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    // 重写方法

    private var theMap = mutable.Map[String, Long]()

    // 判断是否为初始状态
    override def isZero: Boolean = {
      theMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyWordCountAccumulator
    }

    override def reset(): Unit = {
      theMap.clear()
    }

    override def add(word: String): Unit = {
      val newCnt = theMap.getOrElse(word, 0L) + 1
      theMap.update(word, newCnt)
    }


    // 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.theMap
      val map2 = other.value

      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = theMap
  }

}
