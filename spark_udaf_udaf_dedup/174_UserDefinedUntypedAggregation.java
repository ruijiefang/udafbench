//https://raw.githubusercontent.com/lqshow/learning-spark/480e9c5e4dd49616775d8fbc03efbac961c374aa/src/main/java/com/example/spark/sql/UserDefinedUntypedAggregation.java
package com.example.spark.sql;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Spark为所有的UDAF定义了一个父类UserDefinedAggregateFunction
 */
public class UserDefinedUntypedAggregation {

    public static class MyAverage extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage() {
            // 定义与DataFrame列有关的输入Schema,注意传入列的数据类型必须符合事先的设置
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            // 定义存储聚合运算时产生的中间数据结果的Schema
            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }
        // Data types of input arguments of this aggregate function
        public StructType inputSchema() {
            return inputSchema;
        }
        // Data types of values in the aggregation buffer
        public StructType bufferSchema() {
            return bufferSchema;
        }
        // The data type of the returned value
        public DataType dataType() {
            // 定义UDAF 函数的返回值类型
            return DataTypes.DoubleType;
        }
        // Whether this function always returns the same output on the identical input
        public boolean deterministic() {
            return true;
        }
        // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
        // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
        // the opportunity to update its values. Note that arrays and maps inside the buffer are still
        // immutable.
        public void initialize(MutableAggregationBuffer buffer) {
            // 对聚合运算中间结果的初始化
            buffer.update(0, 0L);
            buffer.update(1, 0L);
        }
        // Updates the given aggregation buffer `buffer` with new input data from `input`
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                long updatedSum = buffer.getLong(0) + input.getLong(0);
                long updatedCount = buffer.getLong(1) + 1;
                buffer.update(0, updatedSum);
                buffer.update(1, updatedCount);
            }
        }
        // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
            long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
            buffer1.update(0, mergedSum);
            buffer1.update(1, mergedCount);
        }
        // Calculates the final result
        public Double evaluate(Row buffer) {
            return ((double) buffer.getLong(0)) / buffer.getLong(1);
        }
    }

    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Dataset<Row> df = spark.read().json("src/main/resources/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        // Register the function to access it
        spark.udf().register("myAverage", new MyAverage());

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
    }
}
