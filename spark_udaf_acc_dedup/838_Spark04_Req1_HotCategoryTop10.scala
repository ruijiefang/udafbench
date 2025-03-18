//https://raw.githubusercontent.com/0x112a/IdeaDir/24ff772cd74c3d6312db1deaf3876b0e2f4ecb47/com.0x112.spark/spark-core/src/main/java/com/spark/core/requtes/Spark04_Req1_HotCategoryTop10.scala
package com.spark.core.requtes

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object Spark04_Req1_HotCategoryTop10 {
  def main(args: Array[String]): Unit = {

    val dataFile = "datas/user_visit_action.txt"

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("request1")

    val sc: SparkContext = new SparkContext(sparkConf)

    val datumRDD = sc.textFile(dataFile)

    datumRDD.cache()

    // TODO 重写accumulator 类加器
    val accumulator = new HotCategoryAccumulator

    sc.register(accumulator)

//    val sourceRDD: RDD[(String, (Int, Int, Int))] = datumRDD.flatMap(
    datumRDD.foreach(

  line => {

    val arr = line.split("_")

    if (arr(6) != "-1") {

      val tuple = (arr(6), (1, 0, 0))
      accumulator.add(tuple)
    } else if (arr(8) != "null") {
      val ids: Array[String] = arr(8).split(",")
      ids.map(id => {
        accumulator.add((id, (0, 1, 0)))
      })
    } else if (arr(10) != "null") {
      val ids = arr(10).split(",")
      ids.map(id => {
        accumulator.add((id, (0, 0, 1)))
      })
    } else {
      Nil
    }

  }
)

    val source: mutable.Map[String, (Int, Int, Int)] = accumulator.value

    val list: immutable.Seq[(String, (Int, Int, Int))] = source.toList

//    val stringToTuple: mutable.Map[String, (Int, Int, Int)] = source.map(
//      data => {
//        (data._1, data._2)
//      }
//    )

    println(source.size)

    source.foreach(println)


    sc.stop()

  }

  class HotCategoryAccumulator extends AccumulatorV2[(String,(Int,Int,Int)),mutable.Map[String,(Int,Int,Int)]]{

    private val hotAcc =  mutable.Map[String, (Int, Int, Int)]()

    override def isZero: Boolean = hotAcc.isEmpty

    override def copy(): AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]] = new HotCategoryAccumulator


    override def reset(): Unit = hotAcc.clear()

    override def add(v: (String, (Int, Int, Int))): Unit = {
      val tuple = hotAcc.getOrElse(v._1, (0, 0, 0))
      hotAcc.update(v._1,(tuple._1+v._2._1,tuple._2+v._2._2,tuple._3+v._2._3))
    }

    override def merge(other: AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]]): Unit = {
      val acc1 = this.hotAcc
      val acc2 = other.value

      acc2.foreach{
        case (id,(a,b,c)) => {
          val tuple = acc1.getOrElse(id, (0, 0, 0))

          acc1.update(id,(tuple._1 + a,tuple._2 + b,tuple._3 + c))

        }
      }
    }

    override def value: mutable.Map[String, (Int, Int, Int)] = hotAcc
  }


}