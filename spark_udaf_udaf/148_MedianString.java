//https://raw.githubusercontent.com/MukulK/test/5e28a78744eef1e117e8adcd9dea8604514ae387/MedianString.java
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.mutable.WrappedArray;



public class MedianString extends UserDefinedAggregateFunction {

	private StructType inputSchema;
	private StructType bufferSchema;

	public MedianString() {
		List<StructField> inputFields = new ArrayList<>();
		inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.StringType, true));
		inputSchema = DataTypes.createStructType(inputFields);

		List<StructField> bufferFields = new ArrayList<>();
		bufferFields.add(DataTypes.createStructField("sum", DataTypes.StringType, true));
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
		return DataTypes.StringType;
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
		buffer.update(0, "");
	}

	// Updates the given aggregation buffer `buffer` with new input data from `input`
	public void update(MutableAggregationBuffer buffer, Row input) {
		if (!input.isNullAt(0)) {
			String values =(String) buffer.get(0);
			if(values != null && values.length()==0) {
				buffer.update(0, input.getString(0));
			} else {
				buffer.update(0, values+"|"+input.getString(0));
			}
		}
	}
	// Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		if(buffer1.get(0)!= null && ((String)buffer1.get(0)).length()>0 && buffer2.get(0)!= null && ((String)buffer2.get(0)).length()>0) {
		buffer1.update(0, buffer1.get(0) +"|" +buffer2.get(0));
		} else if(buffer1.get(0)!= null && ((String)buffer1.get(0)).length()>0 && (buffer2.get(0)== null || ((String)buffer2.get(0)).length()==0)) {
			buffer1.update(0, buffer1.get(0));
		} else if(buffer2.get(0)!= null && ((String)buffer2.get(0)).length()>0 && (buffer1.get(0)== null || ((String)buffer1.get(0)).length()==0)) {
			buffer1.update(0, buffer2.get(0));
		}

	}
	// Calculates the final result
	public String evaluate(Row buffer) {
		String[] arr = ((String)buffer.get(0)).split("\\|");
		List<BigDecimal> list = new ArrayList<>();
		for(String strValue: arr) {
			list.add(new BigDecimal(strValue));
		}
		if(list.size()== 0 ) {
			return "";
		}else if(list.size()==1) {
			return list.get(0).toPlainString();
		} else {
			Collections.sort(list);
			return list.get(list.size()/2).toPlainString();
		}
	}

	public static void main(String[] args) {
		System.setProperty("HADOOP_HOME", "C:\\Users\\Mukul-CS\\Downloads\\spark-3.0.0-bin-hadoop3.2\\spark-3.0.0-bin-hadoop3.2");
		System.setProperty("hadoop.home.dir", "C:\\Users\\Mukul-CS\\Downloads\\spark-3.0.0-bin-hadoop3.2\\spark-3.0.0-bin-hadoop3.2");
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL user-defined DataFrames aggregation example")
				.config("spark.master", "local")
				.getOrCreate();
		// Register the function to access it
		spark.udf().register("Median", new MedianString());

		Dataset<Row> df = spark.read().json("src/employees.json");
		df.createOrReplaceTempView("employees");
		df.show();
		// +-------+------+
		// |   name|salary|
		// +-------+------+
		// |Michael|  3000|
		// |   Andy|  4500|
		// | Justin|  3500|
		// |  Berta|  4000|
		// +-------+------+

		Dataset<Row> result = spark.sql("SELECT Median(salary) as average_salary FROM employees");
		result.show();
		// +--------------+
		// |average_salary|
		// +--------------+
		// |        3750.0|
		// +--------------+
	}
}

