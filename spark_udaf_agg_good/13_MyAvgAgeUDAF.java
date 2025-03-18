//https://raw.githubusercontent.com/scq355/kim/9741192a37bf2fff854d56011670c8a0aa4065b9/spark/src/main/java/com/kim/spark/sql/MyAvgAgeUDAF.java
package com.kim.spark.sql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

public class MyAvgAgeUDAF extends Aggregator<Long, AvgAgeBuffer, Long> {

    @Override
    public AvgAgeBuffer zero() {
        return new AvgAgeBuffer(0L, 0L);
    }

    @Override
    public AvgAgeBuffer reduce(AvgAgeBuffer b, Long in) {
        b.setCount(b.getCount() + 1);
        b.setTotal(b.getTotal() + in);
        return b;
    }

    @Override
    public AvgAgeBuffer merge(AvgAgeBuffer b1, AvgAgeBuffer b2) {
        b1.setTotal(b1.getTotal() + b2.getTotal());
        b1.setCount(b1.getCount() + b2.getCount());
        return b1;
    }

    @Override
    public Long finish(AvgAgeBuffer reduction) {
        return reduction.getTotal() / reduction.getCount();
    }

    @Override
    public Encoder<AvgAgeBuffer> bufferEncoder() {
        return Encoders.bean(AvgAgeBuffer.class);
    }

    @Override
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }
}