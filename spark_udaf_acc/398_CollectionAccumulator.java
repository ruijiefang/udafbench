//https://raw.githubusercontent.com/Blondig/Lero-on-Spark/208fd7d343293f1f0839b950f47b9a1e2d3357e6/core/target/java/org/apache/spark/util/CollectionAccumulator.java
package org.apache.spark.util;
/**
 * An {@link AccumulatorV2 accumulator} for collecting a list of elements.
 * <p>
 * @since 2.0.0
 */
public  class CollectionAccumulator<T extends java.lang.Object> extends org.apache.spark.util.AccumulatorV2<T, java.util.List<T>> {
  // not preceding
  // TypeTree().setOriginal(TypeBoundsTree(TypeTree(), TypeTree()))
  public   CollectionAccumulator ()  { throw new RuntimeException(); }
  public  void add (T v)  { throw new RuntimeException(); }
  public  org.apache.spark.util.CollectionAccumulator<T> copy ()  { throw new RuntimeException(); }
  public  org.apache.spark.util.CollectionAccumulator<T> copyAndReset ()  { throw new RuntimeException(); }
  /**
   * Returns false if this accumulator instance has any values in it.
   * @return (undocumented)
   */
  public  boolean isZero ()  { throw new RuntimeException(); }
  public  void merge (org.apache.spark.util.AccumulatorV2<T, java.util.List<T>> other)  { throw new RuntimeException(); }
  public  void reset ()  { throw new RuntimeException(); }
    void setValue (java.util.List<T> newValue)  { throw new RuntimeException(); }
  public  java.util.List<T> value ()  { throw new RuntimeException(); }
}
