//https://raw.githubusercontent.com/yuriFish/sparkDemo/29c1bccdd476eba2c88d75b7f241d87168d3714c/src/main/scala/com/atguigu/bigdata/spark/sql/SparkSQL05_Load_Save1.scala
package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSQL05_Load_Save1 {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 导入隐式转换，这里的spark其实是环境对象的名称
    // 要求这个对象使用val声明
    import spark.implicits._

    // TODO SparkSQL通用的读取和保存
    // TODO 通用的读取
    // RuntimeException: file:/xxx/input/user.json is not a Parquet file.   Parquet 列式存储
    // SparkSQL通用读取的默认数据格式为Parquet列式存储方式
//    val df: DataFrame = spark.read.load("input/users.parquet")

    // 如果想要改变读取文件的格式，需要特殊的操作 .format("xxx")   JSON => JavaScript Object Notation
    // JSON文件的格式要求整个文件满足JSON的语法规则
    // Spark读取文件默认是以行为单位来读取
    // Spark读取JSON文件时，要求文件中的每一行符合JSON的格式要求
    // 如果文件格式不正确，不会报错，但是解析结果不正确
    val df: DataFrame = spark.read.format("json").load("input/user.json")

    df.show()

    // TODO 通用的保存
    // SparkSQL通用保存的默认数据格式为Parquet列式存储方式
//    df.write.save("output/sparkSQL_Load_Save")

    // 如果想要改变保存文件的格式，需要特殊的操作 .format("xxx")
    // 如果想要在路径已经存在的情况下保存数据，可以使用保存模式 .mode() overwrite, append, ignore
    df.write.mode("append").format("json").save("output/sparkSQL_Load_Save1")

    // TODO 释放对象
    spark.stop()
  }

}
