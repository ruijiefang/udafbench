//https://raw.githubusercontent.com/LoveNui/WebLogs-Analysis-System/fea9cbfc653368e9157798d4657ca6cb5732ba58/ScalaReadAndWrite/src/main/scala/software/analysis/nju/Accumulator/URLAccumulator.scala
package software.analysis.nju.Accumulator

import org.apache.spark.util.AccumulatorV2
import software.analysis.nju.util.ParseMapToJson

import scala.collection.mutable

class URLAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]]{
  private var URLMap: mutable.Map[String, Int] = mutable.Map()
  override def isZero: Boolean = {
    URLMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    URLAccumulator.this
  }

  override def reset(): Unit = {
    URLMap.clear()
  }

  override def add(v: String): Unit = {
    if(URLMap.contains(v)){
      URLMap.update(v, URLMap(v) + 1)
    }
    else{
      URLMap +=(v -> 1)
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    this.value ++=other.value
  }

  override def value: mutable.Map[String, Int] = {
    URLMap
  }

  override def toString(): String = {
    ParseMapToJson.map2Json(URLMap)
  }
}
