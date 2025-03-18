//https://raw.githubusercontent.com/haven-violet/sz1903/16704df7ee2154bbb223a73d8bab4d61ceb8a697/src/main/scala/com/baidu/day13/_02AccumulatorOps.scala
package com.baidu.day13

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author liaojincheng
  * @Date 2020/5/19 21:16
  * @Version 1.0
  * @Description
  * 自定义类加器
  */
object _02AccumulatorOps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("acc").setMaster("local")
    val sc = new SparkContext(conf)

    //自定义累加sum操作,注册累加器
    val acc = new _02AccumulatorOps()
    sc.register(acc, "acc")
    //创建RDD
    val rdd = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9))
    rdd.foreach(x=>acc.add(x))
    println(acc.value)
  }
}

/**
  * 在继承的时候需要设置泛型,第一个泛型类型代表输入类型值,第二代表输出类型值
  */
class _02AccumulatorOps extends AccumulatorV2[Int, Int] {
  //创建一个输出变量
  private var sum:Int = _

  //检测获取的值是否为空
  override def isZero: Boolean = sum == 0

  //拷贝多个新累加器(每个Task都用累加器)
  override def copy(): AccumulatorV2[Int, Int] = {
    //创建累加器
    val acc = new _02AccumulatorOps
    //聚合累加的过程,sum迭代操作
    acc.sum = this.sum
    acc
  }

  //重置累加器,让累加器从0开始
  override def reset(): Unit = {
    this.sum = 0
  }

  //将每个分区中的数据添加(分区中add操作)
  override def add(v: Int): Unit = {
    sum += v
  }

  //最终汇总所有的Task中的累加结果集
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    sum += other.value
  }

  //输出值
  override def value: Int = {
    sum
  }
}

