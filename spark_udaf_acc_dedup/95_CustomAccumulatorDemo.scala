//https://raw.githubusercontent.com/Shen-Xmas/Spark-RDD-Demo/df8b6f615e19d1b226de370022f9b27eedafae2f/src/main/scala/com/ds/other/CustomAccumulatorDemo.scala
package com.ds.other

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import java.util
import java.util.function.Consumer

object CustomAccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("custom acc demo"))
    // 创建自定义累加器对象
    val wordAccumulator = new WordAccumulator // 注册累加器后，就就可以直接使用了
    sc.register(wordAccumulator, "wc")
    sc.textFile("src/main/resources/data/words.txt") // 读取源文件
      .filter(e => e.nonEmpty) // 过滤空行后切分为单词，再处理掉空的数据
      .flatMap(line => {
        line.trim.split("[\\s|\"|,|\\.|]+")
      })
      .filter(_.nonEmpty)
      .foreach(e => wordAccumulator.add(e.trim)) // 调用累加方 法就可以执行上面定义的代码了
    println(wordAccumulator.value)
  }
}

class WordAccumulator extends AccumulatorV2[String, util.HashMap[String, Int]] {
  // 定义一个用来存储中间过程的 Map
  private val map = new util.HashMap[String, Int]()
  // 判断累加器是否是初始值，对于当前案例，执行过数据后 map 中肯 定会有元素，所有累加器为初始值时 map 为空
  override def isZero: Boolean = map.isEmpty
  // 创建一个新的累加器
  override def copy(): AccumulatorV2[String, util.HashMap[String, Int]] = new WordAccumulator
  // 重置累加器，只需要将 map 置空就行了
  override def reset(): Unit = map.clear()
  // 添加一条数据，如果当前有该数据就使其 value + 1，否则就新加 入一条值为 1 的键值对
  override def add(v: String): Unit = if (map.containsKey(v)) map.put(v, map.get(v) + 1) else map.put(v, 1)
  // 当前累加器的 map 与另外一个累加器的 map 合并
  override def merge(other: AccumulatorV2[String, util.HashMap[String, Int]]): Unit = {
    other match {
      case o: WordAccumulator =>
        o.value.entrySet().forEach(new Consumer[util.Map.Entry[String, Int]]  {
          override def accept(t: util.Map.Entry[String, Int]): Unit =
            if (map.containsKey(t.getKey)) {
              map.put(t.getKey, map.get(t.getKey) + t.getValue)
            } else {
              map.put(t.getKey, t.getValue)
            }
        }
        )
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }
  // 最终要返回的值，我们返回 map 就可以了
  override def value: util.HashMap[String, Int] = map

}