//https://raw.githubusercontent.com/mleuthold/coding-examples-with-scala-spark-and-python/b225654df4f680505b38502d5446fab3763be7b2/src/main/scala/MyStructuredStreamingExample1.scala
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

import org.apache.spark.sql.expressions.MutableAggregationBuffer

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

import org.apache.spark.sql.Row

import org.apache.spark.sql.types._



import org.apache.spark.sql.functions._



object MyStructuredStreamingExample1 {

  //Logger.getLogger("org").setLevel(Level.OFF)
  @transient lazy val log = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val df = countWords(lines)

    // Start running the query that prints the running counts to the console
    val query = df.writeStream
      .outputMode("complete")
      .format("console")
      //      .format("memory")
      //      .queryName("tableName")
      .start()

  }

  def countWords(lines: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts: DataFrame = words.groupBy("value").count()

    wordCounts

  }

}