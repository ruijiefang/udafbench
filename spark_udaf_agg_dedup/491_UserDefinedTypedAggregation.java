//https://raw.githubusercontent.com/yym373872996/spark-learning/3e7ea6a47b8524e8ccf2a56f708c79d8383ef540/spark-learning-dataset/src/main/java/person/rulo/spark/learning/dataset/udf/UserDefinedTypedAggregation.java
package person.rulo.spark.learning.dataset.udf;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import person.rulo.spark.learning.common.initializer.SparkSessionInitializer;
import person.rulo.spark.learning.common.pojo.Employee;

/**
 * @Author rulo
 * @Date 2020/8/10 12:13
 */
public class UserDefinedTypedAggregation {

    public static class TypedAverage extends Aggregator<Employee, Average, Double> {

        @Override
        public Average zero() {
            return new Average(0L, 0L);
        }

        @Override
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }

        @Override
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }

        @Override
        public Double finish(Average reduction) {
            return ((double) reduction.getSum()) / reduction.getCount();
        }

        @Override
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSessionInitializer.getInstance("userDefinedTypedAggregation");
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "";
        Dataset<Employee> ds = sparkSession.read()
                .option("multiLine", true).option("mode", "PERMISSIVE")
                .json("src/main/resources/employees.json")
                .as(employeeEncoder);
        ds.show();
        TypedAverage typedAverage = new TypedAverage();
        TypedColumn<Employee, Double> averageSalary  = typedAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();
        sparkSession.stop();
    }
}
