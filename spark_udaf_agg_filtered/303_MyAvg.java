//https://raw.githubusercontent.com/ToyHaoran/sparkdemo/a3e76a86e97b8d12997957bb9ae2dc9d2105042a/src/main/java/com/haoran/MyAvg.java
package com.haoran;

import lombok.Data;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

public class MyAvg extends Aggregator<Integer, MyAvg.Buffer, Double> {

    @Data
    public static class Buffer implements Serializable {
        // 平均值 = sum / count
        // Aggregator<IN, BUF, OUT> Buffer缓存最终结果的中间值。
        private Long sum;
        private Long count;

        public Buffer() {
        }

        public Buffer(Long sum, Long count) {
            this.sum = sum;
            this.count = count;
        }
    }

    @Override
    public Buffer zero() {
        // 初始化Buffer
        return new Buffer(0L,0L);
    }

    @Override
    public Buffer reduce(Buffer b, Integer a) {
        // 分区内聚合
        b.setSum(b.getSum() + a);
        b.setCount(b.getCount() + 1);
        return b;
    }

    @Override
    public Buffer merge(Buffer b1, Buffer b2) {
        // 分区间聚合，只能将b2累加到b1
        b1.setSum(b1.getSum() + b2.getSum());
        b1.setCount(b1.getCount() + b2.getCount());
        return b1;
    }

    @Override
    public Double finish(Buffer reduction) {
        // 根绝Buffer计算最终结果
        return reduction.getSum().doubleValue() / reduction.getCount();
    }

    @Override
    public Encoder<Buffer> bufferEncoder() {
        // 指定Buffer的类型，可用kryo进行优化
        return Encoders.kryo(Buffer.class);
    }

    @Override
    public Encoder<Double> outputEncoder() {
        // 指定输出的类型
        return Encoders.DOUBLE();
    }
}
