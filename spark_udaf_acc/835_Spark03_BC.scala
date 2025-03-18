//https://raw.githubusercontent.com/xuyq0618/bigdata_spark01/4cf04a97056cacf093d92cf1e33fcce00fd90cb4/spark-core/src/main/java/com/alibaba/bigdata/spark/core/Acc/Spark03_BC.scala
package com.alibaba.bigdata.spark.core.Acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_BC {
  def main(args: Array[String]): Unit = {
    //1、建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //2、
    val rdd = sc.makeRDD(List("hello","spark","hello","scala"))
    val map1 = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    //封装广播变量
    val bc = sc.broadcast(map1) //将map1加入广播变量，一个Executor中的task（一个分区对应一个task）共享该变量，不用在每个task中存一份

    //后面使用该广播变量：bc.value


    //    3、关闭连接
    sc.stop()
  }


}
