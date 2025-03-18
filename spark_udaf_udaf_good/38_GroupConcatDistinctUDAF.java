//https://raw.githubusercontent.com/stillcoolme/UBA-Spark/04c07e40973aba93b183267227d34310062cc10d/uba-spark/src/main/java/com/stillcoolme/spark/service/product/udf/GroupConcatDistinctUDAF.java
package com.stillcoolme.spark.service.product.udf;

import com.stillcoolme.spark.constant.Constants;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * 组内拼接去重函数（group_concat_distinct()） !!
 */
public class GroupConcatDistinctUDAF extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {
    private final static Logger LOG = LoggerFactory.getLogger(GroupConcatDistinctUDAF.class);

    private static final long serialVersionUID = -2510776241322950505L;

    // 指定输入数据的字段与类型
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));

    // 指定缓冲数据的字段与类型
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));

    // 指定返回类型
    private DataType dataType = DataTypes.StringType;
    // 指定是否是确定性的
    private boolean deterministic = true;

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    /**
     * 初始化一个内部的自己定义的值,在Aggregate之前每组数据的初始化结果
     * 可以认为是，你自己在内部指定一个初始的值
     * @param buffer
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    /**
     * 更新
     * 可以认为一个一个地将组内的字段值传递进来 实现拼接的逻辑
     * buffer.getString(0)获取的是上一次聚合后的值
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // 缓冲中的已经拼接过的城市信息串
        String bufferCityInfo = buffer.getString(0);
        // 刚刚传递进来的某个城市信息
        String cityInfo = input.getString(0);
        // 在这里要实现去重的逻辑
        // 判断：之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息
        if(!bufferCityInfo.contains(cityInfo)){
            if("".equals(bufferCityInfo)){
                bufferCityInfo += cityInfo;
            } else {
                // 比如1:北京 然后得到 1:北京,2:上海
                bufferCityInfo += Constants.REGEX_DATAAPPEND + cityInfo;
            }
            buffer.update(0, bufferCityInfo);
        }
    }

    /**
     * 合并
     * update操作，可能是针对一个分组内的部分数据，在某个节点上发生的
     * 但是可能一个分组内的数据，会分布在多个节点上处理
     * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);
        for (String cityInfo: bufferCityInfo2.split(Constants.REGEX_DATAAPPEND)) {
            if(!bufferCityInfo1.contains(cityInfo)){
                if("".equals(bufferCityInfo1)) {
                    bufferCityInfo1 += cityInfo;
                } else {
                    bufferCityInfo1 += "," + cityInfo;
                }
            }
        }
        buffer1.update(0, bufferCityInfo1);
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
