//https://raw.githubusercontent.com/zzzzzzzs/ApacheSpark-Self-Study/a43e41a5f1a385363aa1649634296436a4cabb20/SparkCore/src/main/java/spark/core/accAndbc/Spark03_Acc3.scala
package spark.core.accAndbc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_Acc3 {

    def main(args: Array[String]): Unit = {
        // TODO Spark - 累加器
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(
            List(1, 2, 3, 4)
        )

        // 创建累加器
        val sumAcc = sc.longAccumulator("sum")

        val mapRDD = rdd.map(
            num => {
                // 使用累加器
                sumAcc.add(num)
                num
            }
        )

        // 获取累加器的值
        // 少加:转换算子中调用累加器，如果没有行动算子的话，那么不会执行
        // 多加:转换算子中调用累加器，如果没有行动算子的话，那么不会执行
        // 一般情况下，累加器会放在行动算子中操作
        mapRDD.collect()
        mapRDD.collect()
        println(sumAcc.value)

        sc.stop()
    }
}
