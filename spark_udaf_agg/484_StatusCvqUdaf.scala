//https://raw.githubusercontent.com/guanghuihuang88/Data-Currency-Algorithm/e73792a4a6ea64c36aa623f2f7f9fa5ae2cbd990/dataCurrencyAlgorithm/src/main/scala/StatusCvqUdaf.scala
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

object StatusCvqUdaf extends Aggregator[Tuple3[String, String, String], CurrencyGraph, Double] {

  // 时效图构造算法
  // 1.初始化时效图
  def zero: CurrencyGraph = new CurrencyGraph

  // 2.添加实体记录
  def reduce(buffer: CurrencyGraph, tuple: Tuple3[String, String, String]): CurrencyGraph = {
    // buffer为zero
    if(buffer.graph.isEmpty) {
      val zero: CurrencyGraph = StudentGraphsUtil.initCurrencyGraph(tuple._3)
      zero.addAsDistinct(tuple._1, tuple._2)
    }
    // buffer不为zero
    else {
      buffer.addAsDistinct(tuple._1, tuple._2)
    }
  }

  // 3.合并时效图
  def merge(buffer1: CurrencyGraph, buffer2: CurrencyGraph): CurrencyGraph = {
    if(buffer1.graph.isEmpty) {
      buffer2
    }
    else if(buffer2.graph.isEmpty) {
      buffer1
    }
    else {
      buffer1 + buffer2
    }
  }

  // 计算最新值个数
  def finish(reduction: CurrencyGraph): Double = {
    // 时效图剪枝
    reduction.fresh
    // 计算有无最新值
    reduction.computeCVQ
  }

  // 为中间输出值类型指定编码器
  override def bufferEncoder: Encoder[CurrencyGraph] = Encoders.kryo[CurrencyGraph]
  // 为最终输出值类型指定编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble


}