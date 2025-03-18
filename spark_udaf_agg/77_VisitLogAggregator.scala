//https://raw.githubusercontent.com/StanislawVictorovich/etesttask/dad018f4657e0f58611b006041b655347006f94c/src/main/scala/spark/testtask/visitlog/VisitLogAggregator.scala
package spark.testtask.visitlog

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import spark.testtask.snapshot.Snapshot

object VisitLogAggregator extends Aggregator[VisitLogFromJson, Snapshot, Snapshot] {
  def zero: Snapshot = Snapshot.empty

  def reduce(snapshot: Snapshot, log: VisitLogFromJson): Snapshot = snapshot.copy(
    dmpId = log.dmpId,
    countries = snapshot.countries + (log.country -> (snapshot.countries.getOrElse(log.country, 0) + 1)),
    countrySeenTime = snapshot.countrySeenTime + (log.country -> Math.max(snapshot.countrySeenTime.getOrElse(log.country, 0), log.utcDays.toInt)),
    cities = incOptMap(snapshot.cities, log.city),
    citySeenTime = maxOptMap(snapshot.citySeenTime, log.city, log.utcDays),
    genders = incOptMap(snapshot.genders, log.gender.map(_.toInt)),
    yobs = incOptMap(snapshot.yobs, log.yob.map(_.toInt)),
    keywords = snapshot.keywords ++ log.keywords.getOrElse(List.empty).map(kw => kw.toInt -> (snapshot.keywords.getOrElse(kw.toInt, 0) + 1)),
    keywordSeenTime = maxMapList(snapshot.keywordSeenTime, log.keywords, log.utcDays),
    siteIds = incOptMap(snapshot.siteIds, log.siteId.map(_.toInt)),
    siteSeenTime = maxOptMap(snapshot.siteSeenTime, log.siteId.map(_.toInt), log.utcDays),
    pageViews = snapshot.pageViews + 1,
    firstSeen = Math.min(snapshot.firstSeen, log.utcDays.toInt),
    lastSeen = Math.max(snapshot.lastSeen, log.utcDays.toInt)
  )

  private def incOptMap[K](optMap: Option[Map[K, Int]], k: Option[K]) = {
    val map = optMap.getOrElse(Map.empty)
    val pair = k.map(x => (x -> (map.getOrElse(x, 0) + 1)))
    pair map (p => map + p) orElse optMap
  }

  private def maxOptMap[K](optMap: Option[Map[K, Int]], k: Option[K], days: Long) = {
    val map = optMap.getOrElse(Map.empty)
    val pair = k.map(x => (x -> Math.max(map.getOrElse(x, 0), days.toInt)))
    pair map (p => map + p) orElse optMap
  }

  private def maxMapList(map: Map[Int, Int], listOpt: Option[List[Long]], days: Long) = {
    val list = listOpt.getOrElse(List.empty)
    map ++ list.map(l => l.toInt -> Math.max(map.getOrElse(l.toInt, 0), days.toInt))
  }

  def merge(s1: Snapshot, s2: Snapshot): Snapshot = s1 merge s2

  def finish(s: Snapshot): Snapshot = s

  def bufferEncoder: Encoder[Snapshot] = Encoders.product

  def outputEncoder: Encoder[Snapshot] = Encoders.product
}
