//https://raw.githubusercontent.com/0x112a/IdeaDir/24ff772cd74c3d6312db1deaf3876b0e2f4ecb47/com.0x112.spark/spark-core/src/main/java/com/spark/core/add/Spark04_Acc_WordCount.scala
package com.spark.core.add

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {

  def main(args: Array[String]): Unit = {
    val myAccConf = new SparkConf().setMaster("local[*]").setAppName("MyAcc")
    val sc = new SparkContext(myAccConf)

    val rdd = sc.makeRDD(List("hello", "scala", "hello", "spark"))


    //累计器： WordCount
    //创建累加器对象
    //向Spark 进行注册

    val wcAcc = new MyAccumulator()

    sc.register(wcAcc)



    rdd.foreach(
      word => {
        wcAcc.add(word)
      }
    )

    println(wcAcc.value)



    sc.stop()
  }

  /*
    自定义累加器

    1.继承AccumulatorV2[IN,OUT]
    IN : 累加其输入的内容
    OUT ： 累加器返回的内容
    2.重写(实现 )方法
   */

  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]]{

    private val wcMap: mutable.Map[String, Long] = mutable.Map[String, Long]()

    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(v: String): Unit = {
      val newCunt = wcMap.getOrElse(v, 0L) + 1

      wcMap.update(v,newCunt)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach{
        case (word,count) => {
          val cou = map1.getOrElse(word,0L) + count
          map1.update(word,cou)
        }
      }
    }

    override def value: mutable.Map[String, Long] = wcMap
  }

}
