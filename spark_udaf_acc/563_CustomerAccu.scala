//https://raw.githubusercontent.com/Andy-xu-007/Spark1209/09aaa86b87f1658174a56a1c026b4ac0a72ee575/main/scala/org/example/accu/CustomerAccu.scala
package org.example.accu

/**
 * 自定义累加器
 */

import org.apache.spark.util.AccumulatorV2

class CustomerAccu extends AccumulatorV2[Int, Int]{

  // 定义一个属性
  var sum:Int = 0

  // 判断对象是否为空，其实也是判断属性
  override def isZero: Boolean = sum == 0

  // 复制
  override def copy(): AccumulatorV2[Int, Int] = {
    val accu = new CustomerAccu
    accu.sum = this.sum
    accu
  }

  // 重置
  override def reset(): Unit = sum = 0

  // 添加值
  override def add(v: Int): Unit = sum += v

  // 合并Executor端传回来的数据
  override def merge(other: AccumulatorV2[Int, Int]): Unit = this.sum += other.value

  // 返回值
  override def value: Int = sum
}
