//https://raw.githubusercontent.com/LoveNui/WebLogs-Analysis-System/fea9cbfc653368e9157798d4657ca6cb5732ba58/ScalaReadAndWrite/src/main/scala/software/analysis/nju/Accumulator/RequestSecAccumulator.scala
package software.analysis.nju.Accumulator

import org.apache.spark.util.AccumulatorV2
import software.analysis.nju.util.ParseMapToJson

import scala.collection.mutable

class RequestSecAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]]{
  private var RequestSecMap: mutable.Map[String, Int] = mutable.Map()
  override def isZero: Boolean = {
    RequestSecMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    RequestSecAccumulator.this
  }

  override def reset(): Unit = {
    RequestSecMap.clear()
  }

  override def add(v: String): Unit = {
    val sec = v.slice(0, 19)
    if(RequestSecMap.contains(sec)){
      RequestSecMap.update(sec, RequestSecMap(sec) + 1)
    }else{
      RequestSecMap +=(sec -> 1)
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    this.value ++=other.value
  }

  override def value: mutable.Map[String, Int] = {
    RequestSecMap
  }

  override def toString(): String = {
    ParseMapToJson.map2Json(RequestSecMap)
  }

}
