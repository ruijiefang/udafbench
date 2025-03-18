//https://raw.githubusercontent.com/zzzzzzzs/ApacheSpark-Self-Study/a43e41a5f1a385363aa1649634296436a4cabb20/SparkCore/src/main/java/spark/core/accAndbc/Spark03_Acc2.scala
package spark.core.accAndbc

import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_Acc2 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 累加器
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark的累加器只能实现数值的累加吗？
        //    这里的累加器其实表示数据的聚合操作

        // TODO 使用累加器实现WordCount

        // Spark默认提供了累加器
        //sc.longAccumulator
        //sc.longAccumulator ==> Long
        //sc.doubleAccumulator ==> double
       // sc.collectionAccumulator.value ==> List

        // 自定义累加器
        // 1. 创建累加器对象
        val acc = new WordCountAccumulator
        // 2. 向Spark进行注册
        sc.register(acc, "WordCount")

        val rdd = sc.makeRDD(
            List(
                ("Hello", 1), ("Word", 2), ("Hello", 3)
            )
        )

        // 使用累加器，就是循环向累加器中增加数据
        rdd.foreach(
            data => {
                // ("Hello", 1)
                // ("Word", 2)
                // ("Hello", 3)
                acc.add(data)
            }
        )

        // 访问累加器的结果
        println(acc.value)

        sc.stop()
    }
    // 自定义累加器
    // 1. 继承AccumulatorV2，定义泛型
    //    IN : 累加器的输入值的类型 => (String, Int)
    //    OUT : 累加器的返回结果的类型 => mutable.Map[String, Int]
    // 2. 重写方法(6)
    // 3. copy, reset, isZero三个方法是在累加器进行序列化时进行调用的
    //    3.1 Job执行之前如果存在闭包，那么需要进行闭包检测，那么会进行序列化
    //    3.2 将计算逻辑进行封装的时候需要进行序列化
    class WordCountAccumulator extends AccumulatorV2[(String, Int), mutable.Map[String, Int]] {

        private var wordCountMap = mutable.Map[String, Int]()

        // TODO 判断当前的累加器是否为初始状态
        // copyAndReset must return a zero value copy
        override def isZero: Boolean = {
            wordCountMap.isEmpty
        }

        // TODO 复制累加器
        override def copy(): AccumulatorV2[(String, Int), mutable.Map[String, Int]] = {
            //println("copy....")
            new WordCountAccumulator
        }

        // TODO 重置累加器
        override def reset(): Unit = {
            wordCountMap.clear()
        }

        // TODO 向累加器中增加数据
        // ("Hello", 1) => {  ("Hello", 1) }
        // ("Word", 2)  => { ("Hello", 1) ,("Word", 2) }
        // ("Hello", 3) => { ("Hello", 4) ,("Word", 2) }
        override def add(v: (String, Int)): Unit = {
            val word = v._1
            val count = v._2

            val oldCount = wordCountMap.getOrElse(word, 0)
            wordCountMap.put(word, oldCount + count)
        }

        // TODO 合并累加器的值
        override def merge(other: AccumulatorV2[(String, Int), mutable.Map[String, Int]]): Unit = {
            val map1 = wordCountMap
            val map2 = other.value
            // TODO 合并2个Map
            wordCountMap =
                map1.foldLeft(map2){
                    ( map, kv ) => {
                        val word = kv._1
                        val count = kv._2
                        val oldCount = map.getOrElse(word, 0)
                        map.put(word, oldCount + count)
                        map
                    }
            }
        }

        // TODO 返回累计器的结果
        // { ("Hello", 4) ,("Word", 2) }
        override def value: mutable.Map[String, Int] = {
            wordCountMap
        }
    }
}
