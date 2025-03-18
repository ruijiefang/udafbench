//https://raw.githubusercontent.com/144gaurav/big-data-common/f015e42fa566412cd65a3078aa65d6409e36ca51/bigdata/spark-example-module/src/main/java/service/SparkJobContext.java
package service;

import accumulator.AccumulatorHolder;
import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorContext;
import org.apache.spark.util.AccumulatorV2;

import java.util.Map;

public class SparkJobContext {

    private final SparkContext sparkContext;
    private final AccumulatorHolder<String> accumulatorHolder = new AccumulatorHolder<>(false);

    public SparkJobContext(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public void registerAccumulator(String accumulatorid, AccumulatorV2<?,?> accumulator){
        sparkContext.register(accumulator,accumulatorid);
        accumulatorHolder.add(accumulatorid,accumulator);
    }

    public <T> T getAccumulator(String accumulatorId, Class<T> valueType){
        return (T) accumulatorHolder.getAccumulator(valueType.getName(),valueType);
    }

    public Map<String, AccumulatorV2<?,?>> getAccumulators(){
        return accumulatorHolder.getAccumulatorsRegister();
    }

    public AccumulatorHolder<String> getAccumulatorHolder() {
        return accumulatorHolder;
    }

    public void cleanContext(){
        accumulatorHolder.getAccumulatorsRegister().forEach((k,v) -> AccumulatorContext.remove(v.id()));
    }
}
