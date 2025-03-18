//https://raw.githubusercontent.com/zzzzzzzs/ApacheSpark-Self-Study/a43e41a5f1a385363aa1649634296436a4cabb20/SparkCore/src/main/java/spark/sql/Spark04_SparkSQL_JDBC.scala
package spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark04_SparkSQL_JDBC {

    def main(args: Array[String]): Unit = {

        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        // 读取MySQL数据
        val df = spark.read
                .format("jdbc")
                .option("url", "jdbc:mysql://aliyun102:3306/spark-sql")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "000000")
                .option("dbtable", "user")
                .load()
//        df.show

        // 保存数据
        // 创建了一个新表
        df.write
                .format("jdbc")
                .option("url", "jdbc:mysql://aliyun102:3306/spark-sql")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "000000")
                .option("dbtable", "user1")
                .mode(SaveMode.Append)
                .save()


        // TODO 关闭环境
        spark.close()
    }
}
