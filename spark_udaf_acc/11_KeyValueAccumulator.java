//https://raw.githubusercontent.com/144gaurav/big-data-common/f015e42fa566412cd65a3078aa65d6409e36ca51/bigdata/spark-example-module/src/main/java/accumulator/KeyValueAccumulator.java
package accumulator;

import org.apache.spark.util.AccumulatorV2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueAccumulator<K,V extends SparkAccumulator> extends AccumulatorV2<Map<K, V>, Map<K,V>> {

    protected final Map<K, V> accs;

    public KeyValueAccumulator() {
        this.accs = new ConcurrentHashMap<>();
    }
    public KeyValueAccumulator(Map<K, V> accs) {
        this.accs = new ConcurrentHashMap<>(accs);
    }

    @Override
    public boolean isZero() {
        return accs.isEmpty();
    }

    @Override
    public AccumulatorV2<Map<K, V>, Map<K, V>> copy() {
        return new KeyValueAccumulator<K,V>(accs);
    }

    @Override
    public void reset() {
        accs.clear();
    }

    @Override
    public void add(Map<K, V> other) {
        other.forEach((k,v) -> accs.merge(k, v, (v1,v2) -> (V) v2.merge(v1)));
    }

    @Override
    public void merge(AccumulatorV2<Map<K, V>, Map<K, V>> other) {
        this.add(other.value());
    }

    @Override
    public Map<K, V> value() {
        return accs;
    }
}
