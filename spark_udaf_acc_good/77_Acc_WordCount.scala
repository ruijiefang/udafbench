//https://raw.githubusercontent.com/iamhwcc/spark_project/4e07ad8c51d38bb922fc1769de5c18f36b2252c7/src/main/scala/Accumulator/Acc_WordCount.scala
package Accumulator

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Acc_WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Acc_WordCount")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD(List("Hello", "Spark", "Flink", "Hello"))

        // 创建累加器对象
        // 向Spark注册
        val wcAcc = new MyAccumulator()
        sc.register(wcAcc, "WordCount_Accumulator")

        rdd.foreach(
            word => {
                //数据的累加，使用累加器
                wcAcc.add(word)
            }
        )
        //获取累加结果
        println(wcAcc.value)

        sc.stop()
    }

    //自定义数据累加器，实现WordCount
    /*
       1.继承AccumulatorV2，定义泛型
         IN：累加器输入数据类型
         OUT：累加器返回的数据类型
       2.重写方法
    */
    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
        var wcMap = mutable.Map[String, Long]()

        //Scala或Spark中Zero大概率表示初始值或初始状态
        //判断是否为初始状态
        override def isZero: Boolean = {
            wcMap.isEmpty
            //如果是空，就是初始状态
        }

        //复制一个累加器
        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            new MyAccumulator()
        }

        //重置，清空Map
        override def reset(): Unit = {
            wcMap.clear()
        }

        //获取累加器所需要计算的值
        override def add(word: String): Unit = {
            val cnt = wcMap.getOrElse(word, 0L) + 1L
            wcMap.update(word, cnt)
        }

        //Driver合并多个累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val map1 = this.wcMap
            val map2 = other.value
            //对于map2的每个(word,count)，更新到map1中
            map2.foreach {
                case (word, count) => {
                    val cnt = map1.getOrElse(word, 0L) + count
                    map1.update(word, cnt)
                }
            }
        }

        //累加器结果
        override def value: mutable.Map[String, Long] = {
            //直接返回上面add的Map
            wcMap
        }
    }
}
