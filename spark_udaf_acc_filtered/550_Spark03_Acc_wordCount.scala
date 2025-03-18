//https://raw.githubusercontent.com/smile-j/bigdata_demo/6799871ec7fdcecb23f6e2f5411251938fb33ebf/spark_core/src/main/java/com/demo/bigdata/spark_core/rdd/acc/Spark03_Acc_wordCount.scala
package com.demo.bigdata.spark_core.rdd.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_Acc_wordCount {

  def main(args: Array[String]): Unit = {


    var sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("aa","bb","aa","cc"))

    //累加器
    var wcAcc = new MyAccumulator

    sc.register(wcAcc)

    rdd.foreach(word=>{
      wcAcc.add(word)
    })

    println(wcAcc.value)
    sc.stop()

  }

  class MyAccumulator extends  AccumulatorV2[String,mutable.Map[String,Long]]{

    private var wcWord = mutable.Map[String,Long]()

    //判断是否是初始化状态
    override def isZero: Boolean = {
      wcWord.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator
    }

    override def reset(): Unit = {
      wcWord.clear()
    }

    override def add(v: String): Unit = {

      val newValue = wcWord.getOrElse(v,0L) + 1L
      wcWord.update(v,newValue)

    }

    //合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      val map1 = this.wcWord
      var map2 = other.value

      map2.foreach({
        case (word,count) =>{
          val newV:Long = map1.getOrElse(word,0L)+count
          map1.update(word,newV)
        }
      })

    }

    override def value: mutable.Map[String, Long] = wcWord
  }
}
