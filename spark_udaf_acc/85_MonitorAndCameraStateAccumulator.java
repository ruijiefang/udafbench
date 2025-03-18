//https://raw.githubusercontent.com/lhq-123/Flink-InternetOfVehicles/1b19f33254de3613af43bc0b2db9f2e2ce5effb0/Flink%E8%BD%A6%E8%81%94%E7%BD%91/%E9%A1%B9%E7%9B%AE%E4%BB%A3%E7%A0%81/CarNetWork/VehicleMonitoring/src/main/java/com/alex/common/MonitorAndCameraStateAccumulator.java
package com.alex.common;
import com.alex.constants.Constants;
import com.alex.utils.StringUtil;
import org.apache.spark.metrics.source.AccumulatorSource;
import org.apache.spark.util.AccumulatorMetadata;
import org.apache.spark.util.AccumulatorV2;

/**
 * @author Alex_liu
 * @create 2023-01-29 16:56
 * @Description 自定义累加器要实现AccumulatorParam接口
 */
public class MonitorAndCameraStateAccumulator extends AccumulatorV2 {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean isZero() {
        return false;
    }

    @Override
    public AccumulatorV2 copy() {
        return null;
    }

    @Override
    public void reset() {

    }

    @Override
    public void add(Object v) {

    }

    @Override
    public void merge(AccumulatorV2 other) {

    }

    @Override
    public Object value() {
        return null;
    }
}

