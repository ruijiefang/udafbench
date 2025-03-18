//https://raw.githubusercontent.com/NashTech-Labs/Sparkathon/68b774128a0f0e01d356b4aa181931b91e965aaf/src/main/java/edu/knoldus/introduction/rdd/Shared.java
package edu.knoldus.introduction.rdd;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;

public class Shared {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
        broadcastVar.value();

        Accumulator<Integer> accum = sc.accumulator(0);

        sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));

        accum.value();
    }
}
