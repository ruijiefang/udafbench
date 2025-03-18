//https://raw.githubusercontent.com/haven-violet/sz1903/16704df7ee2154bbb223a73d8bab4d61ceb8a697/src/main/scala/com/baidu/day13/_03AccumulatorOps.scala
package com.baidu.day13

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @Author liaojincheng
  * @Date 2020/5/19 21:39
  * @Version 1.0
  * @Description
  * 自定义累加器做单词计数
  */
object _03AccumulatorOps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("acc").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List("hello","day","hour","tom","hello","hour"))
    //创建并注册累加器
    val acc = new _03AccumulatorOps()
    sc.register(acc, "wc")
    //单词统计
    rdd.foreach(x=> acc.add(x))
    println(acc.value)
  }
}

class _03AccumulatorOps extends AccumulatorV2[String, mutable.HashMap[String, Int]]{
  //首先创建一个空Map
  private val hashAcc = new mutable.HashMap[String, Int]()

  //检查为空
  override def isZero: Boolean = hashAcc.isEmpty

  //拷贝累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new _03AccumulatorOps
    acc.synchronized{
      acc.hashAcc ++= hashAcc
    }
    acc
  }

  //重置
  override def reset(): Unit = hashAcc.clear()

  //局部sum
  override def add(v: String): Unit = {
    hashAcc.get(v) match {
      case None => hashAcc.put(v, 1)
      case Some(a) => hashAcc.put(v, a+1)
    }
  }
  //全局sum
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case v:AccumulatorV2[String, mutable.HashMap[String, Int]] => {
        for((k,v) <- v.value){
          hashAcc.get(k) match {
            case None => hashAcc.put(k, v)
            case Some(a) => hashAcc.put(k, v + a)
          }
        }
      }
    }
  }

  //输出
  override def value: mutable.HashMap[String, Int] = {
    hashAcc
  }
}
