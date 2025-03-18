//https://raw.githubusercontent.com/AlexRogalskiy/kama-spark-scala/24ffa4e1bb9b2e9efcf6675dee5015490b59037c/src/main/scala/dataframe/LongestWord.java
package dataframe;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class LongestWord extends UserDefinedAggregateFunction {

    @Override public StructType inputSchema() {
        return new StructType().add("name", StringType, true);
    }

    @Override public StructType bufferSchema() {
        return new StructType().add("maxLengthWord", StringType, true);
    }

    @Override public org.apache.spark.sql.types.DataType dataType() {
        return StringType;
    }

    @Override public boolean deterministic() {
        return true;
    }

    @Override public void initialize(MutableAggregationBuffer buffer) {
        
        System.out.println(">>> initialize (buffer: " + buffer.toString() + ")");
        // NOTE: Scala's update used under the covers
        buffer.update(0, "");
        
    }

    @Override public void update(MutableAggregationBuffer buffer, Row input) {
        System.out.println(">>> compare (buffer: " + buffer.toString() + " -> input: " + input.toString() + ")");
        String maxWord = buffer.getString(0);
        String currentName = input.getString(0);
        if (currentName.length() > maxWord.length())
            buffer.update(0, currentName);
    }

    @Override public void merge(MutableAggregationBuffer buffer, Row row) {
        System.out.println(">>> merge (buffer: " + buffer.toString() + " -> row: " + row.toString() + ")");
        String maxWord = buffer.getString(0);
        String currentName = row.getString(0);
        if (currentName.length() > maxWord.length())
            buffer.update(0, currentName);
    }

    @Override public Object evaluate(Row buffer) {
        System.out.println(">>> evaluate (buffer: " + buffer.toString() + ")");

        return buffer.getString(0);
    }
}
