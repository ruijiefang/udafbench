//https://raw.githubusercontent.com/iamhwcc/spark_project/4e07ad8c51d38bb922fc1769de5c18f36b2252c7/src/main/scala/SparkCore_Final_Exercise/Require01_3.scala
package SparkCore_Final_Exercise

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Require01_3 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Require01_3").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val acc = new MyAcc()
        sc.register(acc, "MyACC")

        val text = sc.textFile("datas/user_visit_action.txt")

        text.foreach(
            action => {
                val data = action.split("_")
                if (data(6) != "-1") {
                    acc.add((data(6), "click"))
                } else if (data(8) != "null") {
                    val orderData = data(8).split(",")
                    orderData.foreach(id => acc.add((id, "order")))
                } else if (data(10) != "null") {
                    val payData = data(10).split(",")
                    payData.foreach(id => acc.add((id, "pay")))
                }
            })

        val resAcc = acc.value
        val categoryObjs = resAcc.map(_._2)

        categoryObjs.toList.sortWith(
            (l, r) => {
                if (l.clickCnt > r.clickCnt) {
                    true
                } else if (l.clickCnt == r.clickCnt) {
                    if (l.orderCnt > r.orderCnt) {
                        true
                    } else if (l.orderCnt == r.orderCnt) {
                        l.payCnt > r.payCnt
                    } else {
                        false
                    }
                }
                else {
                    false
                }
            }
        ).take(10).foreach(println)
        sc.stop()
    }

    case class HotCategoryObj(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

    /*
        自定义累加器
        IN：（品类ID,行为类型）
        OUT：mutable.Map[String,HotCategory] 根据ID获取并修改类中的其他属性
    */
    class MyAcc extends AccumulatorV2[(String, String), mutable.Map[String, HotCategoryObj]] {
        private val hcMap = mutable.Map[String, HotCategoryObj]()

        override def isZero: Boolean = {
            hcMap.isEmpty
        }

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategoryObj]] = {
            new MyAcc()
        }

        override def reset(): Unit = {
            hcMap.clear()
        }

        override def add(v: (String, String)): Unit = {
            //查看Map中有没有cid，如果有就更新样例类中的值，如果没有就新增置0
            val cid = v._1
            val actionType = v._2
            //查看Map中有没有该cid
            val categoryObj = hcMap.getOrElse(cid, HotCategoryObj(cid, 0, 0, 0))
            //更新样例类中的值
            if (actionType == "click") {
                categoryObj.clickCnt += 1
            } else if (actionType == "order") {
                categoryObj.orderCnt += 1
            } else if (actionType == "pay") {
                categoryObj.payCnt += 1
            }
            hcMap.update(cid, categoryObj)
        }


        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategoryObj]]): Unit = {
            val map1 = this.hcMap
            val map2 = other.value
            //合并两个map
            //遍历map2中所有
            map2.foreach {
                case (cid, obj) => {
                    //查看map1中有没有该cid
                    val obj1 = map1.getOrElse(cid, HotCategoryObj(cid, 0, 0, 0))
                    //如果有，合并
                    obj1.clickCnt += obj.clickCnt
                    obj1.orderCnt += obj.orderCnt
                    obj1.payCnt += obj.payCnt
                    map1.update(cid, obj1)
                }
            }
        }

        override def value: mutable.Map[String, HotCategoryObj] = {
            hcMap
        }
    }
}
