//https://raw.githubusercontent.com/iserbaev/scala_things/70c3d39b1cf12e43190fd2411e07c8176ffe0aba/spark_stepik/src/main/scala/spark_stepik/ds/CarsDS.scala
package spark_stepik.ds

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import spark_stepik.SparkCxt

object CarsDS extends App with SparkCxt {
  final case class Car(
      id: Int,
      price: Int,
      brand: String,
      `type`: String,
      mileage: Option[Double],
      color: String,
      date_of_purchase: String
  )
  object Car {
    implicit val carStructType: StructType = Encoders.product[Car].schema
  }

  val avgMileageAggregator = new Aggregator[Car, (Int, Double), Double] {
    override def zero: (Int, Double) = (0, 0.0)

    override def reduce(b: (Int, Double), a: Car): (Int, Double) =
      (b._1 + 1, b._2 + a.mileage.getOrElse(0.0))

    override def merge(b1: (Int, Double), b2: (Int, Double)): (Int, Double) =
      (b1._1 + b2._1, b1._2 + b2._2)

    override def finish(reduction: (Int, Double)): Double = reduction._2 / reduction._1

    override def bufferEncoder: Encoder[(Int, Double)] =
      Encoders.tuple[Int, Double](Encoders.scalaInt, Encoders.scalaDouble)

    override def outputEncoder: Encoder[Double] =
      Encoders.scalaDouble
  }.toColumn.name("avg_mileage")

  def withYearSincePurchase(ds: Dataset[Car]): DataFrame =
    ds
      .withColumn(
        "years_since_purchase",
        getFullYearSinceNow("date_of_purchase")
      )

  def getFullYearSinceNow(columnName: String): Column =
    round(
      datediff(current_date(), parseDate(columnName))
        .divide(365),
      0
    )

  def parseDate(columnName: String): Column =
    when(to_date(col(columnName), "yyyy-MM-dd").isNotNull, to_date(col(columnName), "yyyy-MM-dd"))
      .when(to_date(col(columnName), "yyyy MM dd").isNotNull, to_date(col(columnName), "yyyy MM dd"))
      .when(to_date(col(columnName), "yyyy MMM dd").isNotNull, to_date(col(columnName), "yyyy MMM dd"))
      .otherwise("Unknown Format")
      .as("Formatted Date")

  val carsDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("spark_stepik/src/main/resources/2_ds_files/cars.csv")

  import spark.implicits._

  val carsDS = carsDF.as[Car]

  val avgMileageAggregeatedDS: Dataset[Double] = carsDS.select(avgMileageAggregator)

  val resultCarsDS = avgMileageAggregeatedDS
    .join(carsDS.transform(withYearSincePurchase))

  resultCarsDS.show()
}
