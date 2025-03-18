//https://raw.githubusercontent.com/Blondig/Lero-on-Spark/208fd7d343293f1f0839b950f47b9a1e2d3357e6/core/target/java/org/apache/spark/metrics/source/AccumulatorSource.java
package org.apache.spark.metrics.source;
/**
 * AccumulatorSource is a Spark metric Source that reports the current value
 * of the accumulator as a gauge.
 * <p>
 * It is restricted to the LongAccumulator and the DoubleAccumulator, as those
 * are the current built-in numerical accumulators with Spark, and excludes
 * the CollectionAccumulator, as that is a List of values (hard to report,
 * to a metrics system)
 */
  class AccumulatorSource implements org.apache.spark.metrics.source.Source {
  public   AccumulatorSource ()  { throw new RuntimeException(); }
  public  com.codahale.metrics.MetricRegistry metricRegistry ()  { throw new RuntimeException(); }
  protected <T extends java.lang.Object> void register (scala.collection.immutable.Map<java.lang.String, org.apache.spark.util.AccumulatorV2<?, T>> accumulators)  { throw new RuntimeException(); }
  public  java.lang.String sourceName ()  { throw new RuntimeException(); }
}
