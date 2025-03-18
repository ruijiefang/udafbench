//https://raw.githubusercontent.com/veribilimiokulu/udemy-apache-spark/8e3693b98d750720ea01703c50880886b7f17c53/kurs_hazirlik/scala/spark_temel/src/main/scala/sparkTemel/RDD/Accumulators.scala
package sparkTemel.RDD

import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.util.AccumulatorV2

object Accumulators {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

  /*  val sc = new SparkContext("local[4]","Accumulators")

    println("\nProducts: ")
    val productsRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\udemy-apache-spark\\data\\retail_db\\products.csv")
    productsRDD.take(5).foreach(println(_))

    val blankLines = new AccumulatorV2[Int] {}
    var blankLines = sc.longAccumulator("blankLines")

    val callSigns = productsRDD.flatMap(line  => {
      if(line == ""){
        blankLines += 1
      }
      line.split(",")
    })

    println("Black lines: " + blankLines.value)





    sc.stop()
  */
  }
}
