//https://raw.githubusercontent.com/zzzzzzzs/ApacheSpark-Self-Study/a43e41a5f1a385363aa1649634296436a4cabb20/SparkCore/src/main/java/spark/core/accAndbc/Spark04_Bc.scala
package spark.core.accAndbc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Bc {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 广播变量
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(
            List(
                ("a", 1), ("b", 2), ("c", 3)
            )
        )
        val map = Map(
            ("a", 4), ("b", 5), ("c", 6)
        )

        // (a, (1,4)), (b, (2, 5)), (c, (3,6))
        //val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

        // join 会产生笛卡尔乘积，数据量可能会非常的大
        // 数据有可能被打乱重新组合，中间可能会有shuffle
        //println(joinRDD.collect.mkString(","))

        // ("a", 1), ("b", 2), ("c", 3)
        // (a, (1,4)), (b, (2, 5)), (c, (3,6))
        rdd1.map{
            case ( k, v ) => {
                var v1 = map.getOrElse(k, 0)
                (k, (v, v1))
            }
        }.collect.foreach(println)

        sc.stop()
    }
}
