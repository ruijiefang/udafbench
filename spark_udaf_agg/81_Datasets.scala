//https://raw.githubusercontent.com/manmartgarc/coursera-scala/b3275a14047cc6061614c201eb673c9f33d1acdd/course4/lectures/src/main/scala/week4/Datasets.scala
package week4

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

object Datasets {

  case class Listing(street: String, zip: Int, price: Int)
  val listings = Seq(
    Listing("1 Main St", 100, 200000),
    Listing("2 Main St", 200, 300000),
    Listing("3 Main St", 300, 400000),
    Listing("4 Main St", 400, 500000),
    Listing("5 Main St", 500, 600000),
    Listing("6 Main St", 600, 700000),
    Listing("7 Main St", 700, 800000),
    Listing("8 Main St", 800, 900000),
    Listing("9 Main St", 900, 1000000),
    Listing("10 Main St", 1000, 1100000)
  )

  @main def listMain: Unit =
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:/Users/manma")

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("dfs")
        .master("local[*]")
        .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits.{StringToColumn, localSeqToDatasetHolder}
    import scala3encoders.given

    @transient lazy val sc = spark.sparkContext
    val rdd = sc.parallelize(listings)
    import spark.implicits._
    val df = rdd.toDS().as[Listing]
    // with datasets API we can combine RDD and Dataframes APIs!
    df.groupByKey(l => l.zip).agg(avg($"price").as[Double]).show()

    val keyValues = List(
      (3, "Me"),
      (1, "This"),
      (2, "Se"),
      (1, "ssa"),
      (1, "sIsA"),
      (3, "ge:"),
      (3, "-)"),
      (2, "cre"),
      (2, "t")
    )

    val keyValuesDS = keyValues.toDS

    val strConcat = new Aggregator[(Int, String), String, String] {
      def zero: String = ""
      def reduce(b: String, a: (Int, String)): String = b + a._2
      def merge(b1: String, b2: String): String = b1 + b2
      def finish(r: String): String = r
      override def bufferEncoder: Encoder[String] = Encoders.STRING
      override def outputEncoder: Encoder[String] = Encoders.STRING
    }.toColumn

    keyValuesDS
      .groupByKey(pair => pair._1)
      .agg(strConcat.as[String])
      .orderBy($"key")
      .show()
}
