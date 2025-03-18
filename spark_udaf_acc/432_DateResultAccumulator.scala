//https://raw.githubusercontent.com/LoveNui/WebLogs-Analysis-System/fea9cbfc653368e9157798d4657ca6cb5732ba58/ScalaReadAndWrite/src/main/scala/software/analysis/nju/Accumulator/DateResultAccumulator.scala
package software.analysis.nju.Accumulator

import org.apache.spark.util.AccumulatorV2
import software.analysis.nju.Entity.Entity.DateResult

import scala.collection.mutable

class DateResultAccumulator extends AccumulatorV2[DateResult, mutable.Map[String, DateResult]]{
  val resultMap: mutable.Map[String, DateResult] = mutable.Map()
  override def isZero: Boolean = resultMap.isEmpty

  override def copy(): AccumulatorV2[DateResult, mutable.Map[String, DateResult]] = DateResultAccumulator.this

  override def reset(): Unit = resultMap.clear()

  override def add(v: DateResult): Unit = {
    resultMap +=(v.rowK -> v)
  }

  override def merge(other: AccumulatorV2[DateResult, mutable.Map[String, DateResult]]): Unit = {
    this.value ++=other.value
  }

  override def value: mutable.Map[String, DateResult] = resultMap
}
