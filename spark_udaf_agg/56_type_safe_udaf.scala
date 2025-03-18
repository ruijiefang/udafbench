//https://raw.githubusercontent.com/iamhwcc/spark_project/4e07ad8c51d38bb922fc1769de5c18f36b2252c7/src/main/scala/SparkSQL/DataFrame_DataSet/type_safe_udaf.scala
package SparkSQL.DataFrame_DataSet

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn, functions}

object type_safe_udaf {
    // 聚合缓冲区
    case class Average(var sum: Long, var cnt: Integer)
    // 定义类型
    case class employees(name: String, salary: Long)

    object myAvg extends Aggregator[employees, Average, Double] {

        // 初始化缓冲区
        override def zero: Average = Average(0, 0)
        // 聚合
        override def reduce(b: Average, a: employees): Average = {
            b.sum += a.salary
            b.cnt += 1
            b
        }
        // 合并缓冲区
        override def merge(b1: Average, b2: Average): Average = {
            b1.sum += b2.sum
            b1.cnt += b2.cnt
            b1
        }
        // 结果运算逻辑
        override def finish(reduction: Average): Double = {
            reduction.sum / reduction.cnt
        }
        // Encoders编码「固定」
        override def bufferEncoder: Encoder[Average] = Encoders.product[Average]
        override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }


    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("test_dataframe_dataset")
            .config("spark.driver.host","localhost")
            .getOrCreate()
        import spark.implicits._
        val ds: Dataset[employees] = spark.read.json("/Users/hwc/Documents/Spark Project/Spark_WordCount/datas/employees.json").as[employees]
        ds.createTempView("user")

        // Type-safe register the function to access it
        // 强类型的udaf
        val myavg: TypedColumn[employees, Double] = myAvg.toColumn.name("myavg")

        // 强类型的udaf只能用在API的select
        ds.select(myavg).show(false)

        spark.close()
    }
}
