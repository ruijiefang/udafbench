//https://raw.githubusercontent.com/JianXinyu/spark-learn/4db6762dd7643a24280f4320f115159bf5ee66a3/spark-core/src/main/java/practice/Req1_HotCategoryTop10_Improve3.scala
package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Req1_HotCategoryTop10_Improve3 {

  def main(args : Array[String]) : Unit = {
    // TODO: Top10热门品类数量
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1_HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // Problem:
    // 能不能一个 shuffle 都没有？
    // 用累加器！
    
    // 1. import data
    val actionRDD = sc.textFile("data/user_visit_action.txt")

    val acc = new HotCategoryAccumulator
    sc.register(acc, "HotCategory")

    // 2. 将数据转换结构
    actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 点击
          acc.add( (datas(6), "click") )
        } else if (datas(8) != "null") {
          // 下单
          val ids = datas(8).split(",")
          ids.foreach(
            id => {
            acc.add( (id, "order") )
          })
        } else if (datas(10) != "null") {
          // 支付
          val ids = datas(10).split(",")
          ids.foreach(id => {
            acc.add((id, "pay"))
          })
        }
      }
    )


    val accVal: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accVal.map(_._2)

    val sort: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            if (left.payCnt > right.payCnt) {
              true
            } else {
              false
            }
          } else {
            false
          }
        } else {
          false
        }
      }
    )

    // 4. 输出结果
    sort.foreach(println)

    sc.stop()
  }

  case class HotCategory (cid : String, var clickCnt : Int, var orderCnt : Int, var payCnt : Int)
  /**
   * 自定义累加器
   * 1. 继承 AccumulatorV2，定义泛型
   *    IN: ( 品类ID，行为类型 ）
   *    OUT: mutable.map[String, HotCategory]
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]{
    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val action = v._2
      val category = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if ( action == "click" ) {
        category.clickCnt += 1
      } else if ( action == "order" ) {
        category.orderCnt += 1
      } else if ( action == "pay" ) {
        category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value

      map2.foreach{
        case ( cid, hc ) => {
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }
}
