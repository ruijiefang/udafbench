//https://raw.githubusercontent.com/wxguo-shou/bigdata/e1a71297b6dd201587ec9071f09d2f30a4f480e4/spark/atguigu-classes/spark-core/src/main/scala/com/atguigu/spark/sql/Spark04_JDBC.scala
package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author name 婉然从物
 * @create 2023-11-12 20:26
 */
object Spark04_JDBC {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSql的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._



    // TODO 关闭环境
    spark.close()
  }



}
