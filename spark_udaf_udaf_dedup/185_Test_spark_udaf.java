//https://raw.githubusercontent.com/qingfengliu/liuqf_big_data_study/761acbc0ee46ae7f3106ea6a0629238e8d6f83d5/studyflink/spark_lliuqf_test_udf/src/main/java/org/spark_liuqf_test_udaf01/Test_spark_udaf.java
package org.spark_liuqf_test_udaf01;


import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Test_spark_udaf extends UserDefinedAggregateFunction {
    //编写spark自定义udaf函数，求平均值
    //输入数据类型
    @Override
    public StructType inputSchema() {
        return new StructType().add("inputColumn", DataTypes.DoubleType);
    }

    //缓冲区数据类型
    @Override
    public StructType bufferSchema() {
        return new StructType().add("sum", DataTypes.DoubleType).add("count", DataTypes.LongType);
    }

    //返回数据类型
    @Override
    public org.apache.spark.sql.types.DataType dataType() {
        return DataTypes.DoubleType;
    }

    //是否是确定性的
    @Override
    public boolean deterministic() {
        return true;
    }

    //初始化缓冲区
    @Override
    public void initialize(org.apache.spark.sql.expressions.MutableAggregationBuffer buffer) {
        buffer.update(0, 0.0);
        buffer.update(1, 0L);
    }

    //更新缓冲区
    @Override
    public void update(org.apache.spark.sql.expressions.MutableAggregationBuffer buffer, org.apache.spark.sql.Row input) {
        if (!input.isNullAt(0)) {
            buffer.update(0, buffer.getDouble(0) + input.getDouble(0));
            buffer.update(1, buffer.getLong(1) + 1);
        }
    }

    //合并缓冲区
    @Override
    public void merge(org.apache.spark.sql.expressions.MutableAggregationBuffer buffer1, org.apache.spark.sql.Row buffer2) {
        buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0));
        buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
    }

    //计算最终结果
    @Override
    public Object evaluate(org.apache.spark.sql.Row buffer) {
        return buffer.getDouble(0) / buffer.getLong(1);
    }

    //udaf函数的名称
    @Override
    public String toString() {
        return "test_spark_udaf";
    }

}
