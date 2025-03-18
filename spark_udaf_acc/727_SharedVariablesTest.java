//https://raw.githubusercontent.com/Nayanzin-Alexander/spark-java/6d9449ace3a7042681ddbf7e2a4a2aa77e03a1b7/src/test/java/com/nayanzin/sparkjava/ch01basics/SharedVariablesTest.java
package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.spark.api.java.JavaPairRDD.toRDD;
import static org.apache.spark.api.java.JavaRDD.fromRDD;
import static org.assertj.core.api.Assertions.assertThat;

public class SharedVariablesTest extends SharedJavaSparkContext implements Serializable {

    @Test
    public void accumulatorVariableTest() {
        AccumulatorV2<Integer, Integer> totalCharacters = jsc().accumulator(0).newAcc();
        jsc().parallelize(asList("Hello World!!!", "Hello Spark"), 100)
                .map(line -> line.replace(" ", ""))
                .foreach(line -> totalCharacters.add(line.length()));
        assertThat(totalCharacters.value()).isEqualTo(23);
    }

    @Test
    public void broadcastVariableTest() {

        Map<Long, String> countryDictionary = new HashMap<>();
        countryDictionary.put(1L, "Great Britain");
        countryDictionary.put(2L, "USA");
        countryDictionary.put(3L, "France");
        countryDictionary.put(4L, "Germany");

        Broadcast<Map<Long, String>> countryDictionaryBroadcast = jsc().broadcast(countryDictionary);

        JavaPairRDD<String, Long> capitals = jsc().parallelizePairs(asList(
                new Tuple2<>("London", 1L),
                new Tuple2<>("Washington", 2L),
                new Tuple2<>("Paris", 3L),
                new Tuple2<>("Berlin", 4L)));

        JavaPairRDD<String, String> actualCountryCity = capitals
                .mapToPair(capital -> {
                    String country = countryDictionaryBroadcast.getValue().get(capital._2);
                    return new Tuple2<>(country, capital._1);
                });
        JavaPairRDD<String, String> expectedCountryCity = jsc().parallelizePairs(asList(
                new Tuple2<>("Great Britain", "London"),
                new Tuple2<>("USA", "Washington"),
                new Tuple2<>("France", "Paris"),
                new Tuple2<>("Germany", "Berlin")));
        ClassTag<Tuple2<String, String>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(actualCountryCity), tag),
                fromRDD(toRDD(expectedCountryCity), tag));
    }
}
