//https://raw.githubusercontent.com/yym373872996/spark-learning/3e7ea6a47b8524e8ccf2a56f708c79d8383ef540/spark-learning-dataset/src/main/java/person/rulo/spark/learning/dataset/udf/UserDefinedUntypedAggregation.java
package person.rulo.spark.learning.dataset.udf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import person.rulo.spark.learning.common.initializer.SparkSessionInitializer;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author rulo
 * @Date 2020/8/10 11:07
 */
public class UserDefinedUntypedAggregation {

    public static class UnTypedAverage extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public UnTypedAverage() {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }

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
            return DataTypes.DoubleType;
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        /**
         * 初始化，sum = 0，count = 0
         * @param buffer
         */
        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0L);
            buffer.update(1, 0L);
        }

        /**
         * 每个分区内的聚合操作，sum += input，count += 1
         * @param buffer
         * @param input
         */
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                long updatedSum = buffer.getLong(0) + input.getLong(0);
                long updatedCount = buffer.getLong(1) + 1;
                buffer.update(0, updatedSum);
                buffer.update(1, updatedCount);
            }
        }

        /**
         * 合并分区时的操作，sum = sum1 + sum2，count = count1 + count2
         * @param buffer1
         * @param buffer2
         */
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
            long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
            buffer1.update(0, mergedSum);
            buffer1.update(1, mergedCount);
        }

        /**
         * 计算最终结果，avg = sum / count
         * @param buffer
         * @return
         */
        @Override
        public Object evaluate(Row buffer) {
            return ((double) buffer.getLong(0)) / buffer.getLong(1);
        }
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSessionInitializer.getInstance("userDefinedUntypedAggregation");
        sparkSession.udf().register("unTypedAverage", new UnTypedAverage());
        Dataset<Row> df = sparkSession.read()
                .option("multiLine", true).option("mode", "PERMISSIVE")
                .json("src/main/resources/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();
        Dataset<Row> result = sparkSession.sql("SELECT unTypedAverage(salary) AS average_salary FROM employees");
        result.show();
        sparkSession.stop();
    }
}
