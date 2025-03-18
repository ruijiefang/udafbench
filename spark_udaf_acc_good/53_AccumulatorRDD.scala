//https://raw.githubusercontent.com/NeoZephyr/coffe/21d81f4d9afa5987cee4206a955a5fcdd6404072/data-learning/src/main/scala/com/pain/core/spark/rdd/AccumulatorRDD.scala
package com.pain.core.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import java.util

object AccumulatorRDD {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("accumulator").setMaster("local[*]")
        val sparkContext = new SparkContext(sparkConf)
        val accumulator = new LogAccumulator
        sparkContext.register(accumulator, "spark")
        val rdd: RDD[String] = sparkContext.makeRDD(List("Hello spark", "spark stream", "kafka stream", "flink"), 2)

        val filterRdd: RDD[String] = rdd.filter(word => {
            val flag = word.contains("spark")
            if (flag) {
                accumulator.add(word)
            }
            flag
        })
        filterRdd.collect().foreach(println)
        println(accumulator.value)
    }
}

class LogAccumulator extends AccumulatorV2[String, util.Set[String]] {

    private val _logArray = new util.HashSet[String]()

    override def isZero: Boolean = {
        _logArray.isEmpty
    }

    override def copy(): AccumulatorV2[String, util.Set[String]] = {
        val newLogAcc = new LogAccumulator
        _logArray.synchronized {
            newLogAcc._logArray.addAll(_logArray)
        }

        newLogAcc
    }

    override def reset(): Unit = {
        _logArray.clear()
    }

    override def add(v: String): Unit = {
        _logArray.add(v)
    }

    override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
        other match {
            case o => _logArray.addAll(o.value)
        }
    }

    override def value: util.Set[String] = {
        java.util.Collections.unmodifiableSet(_logArray)
    }
}
