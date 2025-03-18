//https://raw.githubusercontent.com/EthanLee0328/spark/32892b6ec65031e213f6f57d898645c85e0a26e5/code/atguigu/spark-core/src/main/scala/com/ethan/accumulator/Spark_Accumulator_Replace_Shuffle.scala
package com.ethan.accumulator

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author EthanLee
 * @Version 1.0
 */
object Spark_Accumulator_Replace_Shuffle {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark_Accumulator_Replace_Shuffle").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val originalRDD = sparkContext.makeRDD(List("Hello", "Hello", "Spark", "Flink", "Ethan"), 2)

    // originalRDD.map((_,1)).reduceByKey(_+_).collect().foreach(println)

    // define the accumulator
    val accumulator = new MyAccumulator
    // register the accumulator
    sparkContext.register(accumulator, "wordCount")
    // use map to replace foreach
    originalRDD.foreach(
      word => {
        accumulator.add(word)
      }
    )
    //accumulator.value
    println(accumulator.value)

    sparkContext.stop()


  }
}

// define the accumulator
class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
  // define the map
  private var map = mutable.Map[String, Long]()

  // if initialize the map
  override def isZero: Boolean = {
    map.isEmpty
  }

  // copy the accumulator
  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
    new MyAccumulator()
  }

  // reset the accumulator
  override def reset(): Unit = {
    map.clear()
  }

  // add the value to the map
  override def add(v: String): Unit = {
    val newCnt = map.getOrElse(v, 0L) + 1L
    map.update(v, newCnt)
  }

  // merge the accumulator
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit ={
    val map01 = this.map
    val map02 = other.value

    map02.foreach {
      case(word,count)=>{
        val newCnt = map01.getOrElse(word,0L)+count
        map01.update(word,newCnt)
      }
    }

  }

  override def value: mutable.Map[String, Long] = {
    map
  }
}