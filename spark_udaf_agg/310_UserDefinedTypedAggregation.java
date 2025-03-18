//https://raw.githubusercontent.com/lqshow/learning-spark/480e9c5e4dd49616775d8fbc03efbac961c374aa/src/main/java/com/example/spark/sql/UserDefinedTypedAggregation.java
package com.example.spark.sql;

import com.example.spark.beans.Average;
import com.example.spark.beans.Employee;
import com.example.spark.helpers.Utils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

public class UserDefinedTypedAggregation {

    public static class MyAverage extends Aggregator<Employee, Average, Double> {
        // A zero value for this aggregation. Should satisfy the property that any b + zero = b
        public Average zero() {
            return new Average(0L, 0L);
        }
        // Combine two values to produce a new value. For performance, the function may modify `buffer`
        // and return it instead of constructing a new object
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }
        // Merge two intermediate values
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }
        // Transform the output of the reduction
        public Double finish(Average reduction) {
            return ((double) reduction.getSum()) / reduction.getCount();
        }
        // Specifies the Encoder for the intermediate value type
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }
        // Specifies the Encoder for the final output value type
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }


    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Dataset<Employee> ds = spark.read().json("src/main/resources/employees.json").as(Encoders.bean(Employee.class));
        ds.show();

        MyAverage myAverage = new MyAverage();
        // Convert the function to a `TypedColumn` and give it a name
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();

    }
}
