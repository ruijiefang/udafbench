//https://raw.githubusercontent.com/NJU-Spark-Team/PM2P5-Stream-Process/83af3989d2b3e0815fe7de6d0bd8ca45dd66684b/src/main/java/component/AverageAccumulator.java
package component;

import org.apache.spark.util.AccumulatorV2;

/**
 * accumulator for computing average across nodes
 *
 * @author Nosolution
 * @version 1.0
 * @since 2019/10/12
 */
public class AverageAccumulator extends AccumulatorV2<Double, Double> {

    private Long n = 0L;
    private Double average = 0.0;

    @Override
    public boolean isZero() {
        return average == 0.0;
    }

    @Override
    public AccumulatorV2<Double, Double> copy() {
        AverageAccumulator aa = new AverageAccumulator();
        aa.n = this.n;
        aa.average = this.average;
        return aa;
    }

    @Override
    public void reset() {
        average = 0.0;
    }

    @Override
    public void add(Double d) {
        average = (n * average + d) / (n + 1);
        n += 1;
    }

    @Override
    public void merge(AccumulatorV2<Double, Double> other) {
        if (other instanceof AverageAccumulator) {
            Double otherAverage = ((AverageAccumulator) other).average;
            Long otherN = ((AverageAccumulator) other).n;

            average = (average * n + otherAverage * otherN) / (n + otherN);
            n += otherN;
        } else {
            throw new UnsupportedOperationException(String.format("Cannot merge %s with %s", this.getClass().toString(), other.getClass().toString()));
        }
    }


    @Override
    public Double value() {
        return average;
    }


}
