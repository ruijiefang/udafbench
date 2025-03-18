//https://raw.githubusercontent.com/sparklyr/sparklyr/6c8a6c4a28f893fd4a7a7dc49fe346d50231d724/java/spark-2.4.0/cumprod.scala
package sparklyr

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CumProd extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(
    StructField("value", DoubleType, true) :: Nil
  )

  override def bufferSchema: StructType = StructType(
    StructField("product", DoubleType, true) :: Nil
  )

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    multiply(buffer, input)
  }

  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    multiply(buffer, input)
  }

  override def evaluate(buffer: Row): Any = {
    if (null == buffer(0)) {
      null
    } else {
      buffer.getAs[Double](0)
    }
  }

  private[this] def multiply(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = (
      if (null == input(0) || null == buffer(0)) {
        null
      } else {
        buffer.getAs[Double](0) * input.getAs[Double](0)
      }
    )
  }
}
