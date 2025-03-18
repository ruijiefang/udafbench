//https://raw.githubusercontent.com/kuzetech/lab/201503691d749be2b64f5d4012f9b92aa307c12b/lab-java-all/spark/src/main/java/com/kuzetech/bigdata/spark/sql/MyAverageList.java
package com.kuzetech.bigdata.spark.sql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.ArrayList;


public class MyAverageList extends Aggregator<Long, AverageList, Double> {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    @Override
    public AverageList zero() {
        return new AverageList(new ArrayList<>());
    }
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    @Override
    public AverageList reduce(AverageList buffer, Long data) {
        buffer.getList().add(data);
        return buffer;
    }
    // Merge two intermediate values
    @Override
    public AverageList merge(AverageList b1, AverageList b2) {
        b1.getList().addAll(b2.getList());
        return b1;
    }
    // Transform the output of the reduction
    @Override
    public Double finish(AverageList reduction) {
        Double sum = 0.0;
        for (Long aLong : reduction.getList()) {
            sum = sum + aLong;
        }
        return sum / reduction.getList().size();
    }
    // Specifies the Encoder for the intermediate value type
    @Override
    public Encoder<AverageList> bufferEncoder() {
        return Encoders.bean(AverageList.class);
    }
    // Specifies the Encoder for the final output value type
    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }
}