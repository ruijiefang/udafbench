//https://raw.githubusercontent.com/scq355/kim/9741192a37bf2fff854d56011670c8a0aa4065b9/spark/src/main/java/com/kim/spark/sql/MyCityRemark.java
package com.kim.spark.sql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class MyCityRemark extends Aggregator<String, MyCityRemarkBuffer ,String> {
    @Override
    public MyCityRemarkBuffer zero() {
        return new MyCityRemarkBuffer(0L, new HashMap<>());
    }

    @Override
    public MyCityRemarkBuffer reduce(MyCityRemarkBuffer buffer, String city) {
        buffer.setCount(buffer.getCount() + 1);
        Map<String, Long> cityMap = buffer.getCityMap();
        Long count = cityMap.get(city);
        if (count == null) {
            cityMap.put(city, 1L);
        } else {
            cityMap.put(city, count + 1);
        }
        return buffer;
    }

    @Override
    public MyCityRemarkBuffer merge(MyCityRemarkBuffer b1, MyCityRemarkBuffer b2) {
        b1.setCount(b1.getCount() + b2.getCount());

        Map<String, Long> b1Map = b1.getCityMap();
        Map<String, Long> b2Map = b2.getCityMap();

        for (Map.Entry<String, Long> b2Entry : b2Map.entrySet()) {
            String k = b2Entry.getKey();
            Long v = b2Entry.getValue();
            if (b1Map.get(k) == null) {
                b1Map.put(k, v);
            } else {
                b1Map.put(k, b1Map.get(k) + v);
            }
        }
        return b1;
    }

    @Override
    public String finish(MyCityRemarkBuffer reduction) {
        StringBuilder builder = new StringBuilder();

        Long total = reduction.getCount();
        Map<String, Long> cityMap = reduction.getCityMap();
        List<CityCount> cityCountList = new ArrayList<>();

        cityMap.forEach((k, v) -> cityCountList.add(new CityCount(k, v)));
        Collections.sort(cityCountList);

        CityCount c0 = cityCountList.get(0);
        double percent0 = BigDecimal.valueOf(((double) c0.getCount() / (double) total) * 100)
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
        builder.append(c0.getCityName() + " " + percent0 + "%");

        CityCount c1 = cityCountList.get(1);
        double percent1 = BigDecimal.valueOf(((double) c1.getCount() / (double) total) * 100)
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
        builder.append(c1.getCityName() + " " + percent1 + "%");

        if (cityCountList.size() > 2) {
            double last = BigDecimal.valueOf(100 - percent1 - percent0)
                    .setScale(2, RoundingMode.HALF_UP).doubleValue();
            builder.append("其他" + " " + last + "%");
        }
        return builder.toString();
    }

    @Override
    public Encoder<MyCityRemarkBuffer> bufferEncoder() {
        return Encoders.bean(MyCityRemarkBuffer.class);
    }

    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}
