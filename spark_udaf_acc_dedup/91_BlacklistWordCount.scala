//https://raw.githubusercontent.com/thedevd/techBlog/4e4bf2de7f9faa4a06401c2b0627c36d9b8c5cd9/sparkexamples/src/main/scala/com/thedevd/sparkexamples/sharedvariables/BlacklistWordCount.scala
package com.thedevd.sparkexamples.sharedvariables

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

/*
 * See /accumulator/blacklistusecase.txt file in resource to know about black list word count
 * problem which we are going to solve here using custom accumulator.
 */
object BlacklistWordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("BlacklistWordCount")
      .master("local[*]")
      .getOrCreate()

    val customAccumulator = new BlackListWordCountAccumulator
    spark.sparkContext.register(customAccumulator, "blacklistwordcount")

    val words = spark.sparkContext.textFile(getClass.getResource("/accumulator/blacklistusecase.txt").getPath, 2)
      .flatMap(line => line.split("\\s+"))
      .map(word => {
        customAccumulator.add(word.replace(".", "")) // some blacklist word is ended with period, so removing it.
      }) 

    words.cache() // this is added after finding gotchas with this. see below for more details.
    
    words.collect() // action need to be called to trigger execution.
    println(customAccumulator.value)
    // Map(nude -> 1, shit -> 2, gay -> 2, naked -> 1, moron -> 2, liquor -> 2)
    
    words.collect() // calling action again
    println(customAccumulator.value)
    // Map(nude -> 1, shit -> 2, gay -> 2, naked -> 1, moron -> 2, liquor -> 2)
    
    
    
    /*
     * Some gotchas here
     * ####################
     * 1. So our output is Map(nude -> 1, shit -> 2, gay -> 2, naked -> 1, moron -> 2, liquor -> 2)
     *    what if we again call action i.e.
     *    
     *    words.collect()
     *    println(customAccumulator.value) // Map(nude -> 1, shit -> 2, gay -> 2, naked -> 1, moron -> 2, liquor -> 2)
     *    
     *    words.collect() // calling action again will trigger accumulator add and value will be added twice
     *    println(customAccumulator.value) // Map(nude -> 2, shit -> 4, gay -> 4, naked -> 2, moron -> 4, liquor -> 4)
     *    
     * To solve this problem to keep accumulator value consistent irrespective of calling action again and again =>
     * 1. Use the action operator of the RDD containing Accumulator once. OR
     * 2. Use the cache or persist method to cache the first calculation result of the RDD containing the Accumulator.
     *    i.e. 
     *    words.cache()
     *    
     *    words.collect()
     *    println(customAccumulator.value) Map(nude -> 1, shit -> 2, gay -> 2, naked -> 1, moron -> 2, liquor -> 2)
     *    
     *    words.collect()
     *    println(customAccumulator.value) Map(nude -> 1, shit -> 2, gay -> 2, naked -> 1, moron -> 2, liquor -> 2)
     *    
     *    so here calling action on cached RDD will not trigger the execution from start.	
     *    
     * 
     */

  }

  // Important methods are add() and merge()
  class BlackListWordCountAccumulator extends AccumulatorV2[String, scala.collection.mutable.HashMap[String, Int]] {

    val blackListWordCountZero = scala.collection.mutable.HashMap("gay" -> 0, "shit" -> 0, "moron" -> 0,
      "naked" -> 0, "nude" -> 0, "liquor" -> 0)

    var blackListWordCountValue = scala.collection.mutable.HashMap("gay" -> 0, "shit" -> 0, "moron" -> 0,
      "naked" -> 0, "nude" -> 0, "liquor" -> 0)

    def add(v: String): Unit = {
      if (v == null || "".equals(v.trim())) return
      if (blackListWordCountValue.keySet.contains(v)) // if it is blacklist word
      {
        val oldValue = blackListWordCountValue.get(v).get
        blackListWordCountValue.put(v, oldValue + 1) // increament count by 1
      }
    }

    def copy(): org.apache.spark.util.AccumulatorV2[String, scala.collection.mutable.HashMap[String, Int]] = {
      val newAccumulator = new BlackListWordCountAccumulator
      for (entry <- blackListWordCountValue) {
        val key = entry._1
        val value = entry._2
        newAccumulator.blackListWordCountValue.put(key, value)
      }
      newAccumulator
    }
    def isZero: Boolean = {
      for (entry <- blackListWordCountValue) {
        val value: Int = entry._2
        if (value != 0) return false // if we find somewhere not 0, means it is not zero, return false
      }
      true
    }

    def merge(other: org.apache.spark.util.AccumulatorV2[String, scala.collection.mutable.HashMap[String, Int]]): Unit = {
      if (other == null) return
      val otherAccumulatorValue = other.value
      for (otherMapEntry <- otherAccumulatorValue) {
        val otherKey = otherMapEntry._1
        val otherValue = otherMapEntry._2

        val currentValue = this.blackListWordCountValue.get(otherKey).get
        val newValue = currentValue + otherValue // merging value here.
        blackListWordCountValue.put(otherKey, newValue) // update new value
      }
    }
    def reset(): Unit = {
      for (blackword <- blackListWordCountValue.keySet) {
        blackListWordCountValue.put(blackword, 0)
      }
    }

    def value: scala.collection.mutable.HashMap[String, Int] = { blackListWordCountValue }

  }
}