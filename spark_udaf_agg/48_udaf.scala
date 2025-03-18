//https://raw.githubusercontent.com/iamhwcc/spark_project/4e07ad8c51d38bb922fc1769de5c18f36b2252c7/src/main/scala/SparkSQL/DataFrame_DataSet/udaf.scala
package SparkSQL.DataFrame_DataSet


import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, functions}

object udaf {

    // 聚合缓冲区
    case class Average(var sum: Long, var cnt: Integer)
    // 定义类型
    case class employees(name: String, salary: Long)

    object myAvg extends Aggregator[Long, Average, Double] {

        // 初始化缓冲区
        override def zero: Average = Average(0, 0)

        override def reduce(b: Average, a: Long): Average = {
            b.sum += a
            b.cnt += 1
            b
        }

        override def merge(b1: Average, b2: Average): Average = {
            b1.sum += b2.sum
            b1.cnt += b2.cnt
            b1
        }

        override def finish(reduction: Average): Double = {
            reduction.sum / reduction.cnt
        }

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
        val df: DataFrame = spark.read.json("/Users/hwc/Documents/Spark Project/Spark_WordCount/datas/employees.json")
        df.createTempView("user")

        // 弱类型的udaf
        spark.udf.register("myavg", functions.udaf(myAvg))

        // SQL中只能使用弱类型的udaf
        val sql: String = """
                               |select myavg(salary)
                               |from user
                               |""".stripMargin
        spark.sql(sql).show()

        spark.close()
    }
}
