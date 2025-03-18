//https://raw.githubusercontent.com/Maicius/WebLogsAnalysisSystem/18a7bada53784b5f09dfa8b278795b3f8dbc63e7/ScalaReadAndWrite/src/main/scala/software/analysis/nju/Accumulator/AllDataAccumulator.scala
package software.analysis.nju.Accumulator

import org.apache.spark.util.AccumulatorV2
import software.analysis.nju.Entity.Entity.{DateResult, CourtResult}

import scala.collection.mutable

class AllDataAccumulator extends AccumulatorV2[CourtResult, mutable.Map[String, CourtResult]]{
  val resultMap: mutable.Map[String, CourtResult] = mutable.Map()
  override def isZero: Boolean = resultMap.isEmpty

  override def copy(): AccumulatorV2[CourtResult, mutable.Map[String, CourtResult]] = AllDataAccumulator.this

  override def reset(): Unit = resultMap.clear()

  override def add(v: CourtResult): Unit = {
    resultMap +=(v.rowK -> v)
  }

  override def merge(other: AccumulatorV2[CourtResult, mutable.Map[String, CourtResult]]): Unit = {
    this.value ++=other.value
  }

  override def value: mutable.Map[String, CourtResult] = resultMap
}
