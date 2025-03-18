//https://raw.githubusercontent.com/yuriFish/sparkDemo/29c1bccdd476eba2c88d75b7f241d87168d3714c/src/main/scala/com/atguigu/bigdata/spark/core/acc_BC/Spark57_RDD_BC01.scala
package com.atguigu.bigdata.spark.core.acc_BC

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark57_RDD_BC01 {

  def main(args: Array[String]): Unit = {
    // TODO Spark 广播变量: 分布式共享只读变量
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    // join 有笛卡尔乘积效果，数据量会急剧增多。如果有shuffle操作，那么性能会非常低
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    println(joinRDD.collect().mkString(","))

    sc.stop()
  }


}
