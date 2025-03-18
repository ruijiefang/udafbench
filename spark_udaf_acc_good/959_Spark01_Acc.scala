//https://raw.githubusercontent.com/StanLong/Hadoop/fae52e822b471153453348c93dec4b3b8fcf0b43/07Spark/Spark/spark-core/src/main/java/com/stanlong/spark/core/acc/Spark01_Acc.scala
package com.stanlong.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark01_Acc {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ACC")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List("Hello", "Spark", "Hive", "Hello"))

        // 创建累加器对象
        val wcAcc = new MyAccumulator

        // 向Spark进行注册
        sc.register(wcAcc)

        // 使用自定义累加器
        rdd.foreach(
            word => {
                wcAcc.add(word)
            }
        )

        println(wcAcc.value)



        sc.stop()


    }

    /**
     *  自定义累加器
     *  1. 继承 AccumulatorV2, 定义泛型
     *      IN: 累加器输入的数据类型 String
     *      OUT: 累加器返回的数据类型 mutable.Map(String, Long)
     */
    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{

        private var wcMap = mutable.Map[String, Long]()

        // 判断是否为初始状态
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

        // 合并累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val map1 = this.wcMap
            val map2 = other.value

            map2.foreach{
                case(word, count) =>{
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
