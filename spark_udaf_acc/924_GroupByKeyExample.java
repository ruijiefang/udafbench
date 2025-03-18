//https://raw.githubusercontent.com/asfish/spark/cc676cdfdc58c094db6b929863f6c43808afd458/src/main/java/com/tryit/spark/apps/rdd/GroupByKeyExample.java
package com.tryit.spark.apps.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.SizeEstimator;

import com.google.common.collect.Lists;

import scala.Option;
import scala.Tuple2;

/**
 * Created by agebriel on 12/13/16.
 */
public class GroupByKeyExample
{
	public static void main(String[] args)
	{
		// Parallelized with 2 partitions
		JavaRDD<String> baseRdd = Util.sc().parallelize(
			Arrays.asList("Joseph", "Jimmy", "Tina",
				"Thomas", "James", "Cory",
				"Christine", "Jackeline", "Juan", "Joe", "Tomas", "John"), 2);

		// creating a key-value pairs RDD
		JavaPairRDD<String, String> pairsRDD = baseRdd.mapToPair(new PairFunction<String, String, String>()
		{
			int count = 0;

			@Override
			public Tuple2<String, String> call(String s) throws Exception
			{
				count++;
				if (count <= 3)
					return new Tuple2<>("dev", s);
				else
					return new Tuple2<>("qa", s);
			}
		});

		checkPartitioner(pairsRDD);
		List<Tuple2<String, String>> pairs = pairsRDD.collect();
		for(Tuple2<String, String> t : pairs)
			System.out.println(t._1()+" => "+t._2());

		// grouping
		JavaPairRDD<String, Iterable<String>> groups = pairsRDD.groupByKey();
		checkPartitioner(groups);

		/*check the size of the RDD
		 *
		 * Estimate the number of bytes that the given object takes up on the JVM heap.
		 * The estimate includes space taken up by objects referenced by the given object,
		 * their references, and so on and so forth.
		 *
		 * This is useful for determining the amount of heap space a broadcast variable will
		 * occupy on each executor or the amount of space each object will take when caching
		 * objects in deserialized form. This is not the same as the serialized size of the
		 * object, which will typically be much smaller.
		*/
		System.out.println("The size of 'group' RDD is " + SizeEstimator.estimate(groups) + " bytes");

		List<Tuple2<String, Iterable<String>>> group = groups.collect();
		for(Tuple2<String, Iterable<String>> t : group)
		{
			System.out.println(t._1() + " => "+ t._2());
		}

		// counting elements in groups
		JavaPairRDD<String, Integer> counts = groups.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Integer>()
		{
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> t)
			{
				// I need to wrap the Iterable into a List to get its size
				return new Tuple2<String, Integer>(t._1(), Lists.newArrayList(t._2()).size());
			}
		});

		// fetching results
		List<Tuple2<String, Integer>> result = counts.collect();
		for(Tuple2<String, Integer> t : result)
			System.out.println(t._1()+" => "+t._2());
	}

	private static void checkPartitioner(JavaPairRDD pairRDD)
	{
		Optional<Partitioner> partitioner = pairRDD.partitioner();
		//org.apache.spark.Partitioner p = partitioner.get();

		if(!partitioner.isPresent())
			System.out.println("this pairRdd does not have partitioner");
	}

	private static void accumulate(JavaSparkContext sc)
	{
		Accumulator acc = sc.accumulator(0);
	}
}
