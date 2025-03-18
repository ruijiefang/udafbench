//https://raw.githubusercontent.com/thedevd/techBlog/4e4bf2de7f9faa4a06401c2b0627c36d9b8c5cd9/sparkexamples/src/main/scala/com/thedevd/sparkexamples/sharedvariables/Accumulator.scala
package com.thedevd.sparkexamples.sharedvariables

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

/*
 * 1. Accumulators are another type of shared variables in spark that are used to
 *    perform write operations like counters and sum operations across workers nodes.
 *
 *    One of the most common use of Accumulator is count particular events that may help in debugging process.
 * 2. Accumulator are write-only variables for executors. 
 *    They can be added to by executors and read by the driver only.
 * 
 * 3. Accumulator is created using 
 *    sparkContext.longAccumulator() or sparkContext.doubleAccumulator() or sparkContext.collectionAccumulator()
 * 4. Accumulator is updated using add() method
 * 5. At drive, value() is used to read the final value.
 *
 * For ex: we have input file /sparksql/accumulator.txt consisting blank lines which we want to count.
 *
 */
object Accumulator {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Accumulator")
      .master("local[*]")
      .getOrCreate()
    /*
     * Using the normal variable wont work.
     * var counter = 0  
     * 
     * val blank_lines = spark.sparkContext.textFile(getClass.getResource("/accumulator/accumulator.txt").getPath, 4)
     *    .filter(_.trim().length() == 0)
     *    .foreach(line => counter = counter + 1)
     *    
     * println(counter) // still 0
     * 
     * Why this did not work, because driver sends copy of counter variable to each worker node.
     * And each worker node updates thier local copy of it. And updated value of variable never
     * sends back to driver, that is the reason counter value becomes still 0 at driver side.
     */
    val counter = spark.sparkContext.longAccumulator("blanklines")
    
    val blank_lines = spark.sparkContext.textFile(getClass.getResource("/accumulator/accumulator.txt").getPath, 4)
     .filter(_.trim().length() == 0)
     .foreach(line => counter.add(1)) // add() is used to write into accumulator
     
     
    println("no of blank lines: " + counter.value) // no of blank lines: 4
     
      
  }

}