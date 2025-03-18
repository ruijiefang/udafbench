//https://raw.githubusercontent.com/MukulK/test/5e28a78744eef1e117e8adcd9dea8604514ae387/Median.java
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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



public class Median extends UserDefinedAggregateFunction {

	private StructType inputSchema;
	private StructType bufferSchema;

	public Median() {
		List<StructField> inputFields = new ArrayList<>();
		inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.createDecimalType(), true));
		inputSchema = DataTypes.createStructType(inputFields);

		List<StructField> bufferFields = new ArrayList<>();
		bufferFields.add(DataTypes.createStructField("sum", DataTypes.createArrayType(DataTypes.createDecimalType()), true));
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
		return DataTypes.createDecimalType(30, 9);
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
		List l1 = new ArrayList<BigDecimal>();
		//			  l1.add(new BigDecimal(1234));
		buffer.update(0, l1);
	}

	// Updates the given aggregation buffer `buffer` with new input data from `input`
	public void update(MutableAggregationBuffer buffer, Row input) {
		if (!input.isNullAt(0)) {
			Object[] arr = (Object[]) ((WrappedArray<BigDecimal>)buffer.get(0)).array();
			List<Object> l1 = new ArrayList<Object>();
			l1.addAll(Arrays.asList(arr));
			l1.add(input.getDecimal(0));
			buffer.update(0, l1);
		}
	}
	// Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		List<Object> l1 = new ArrayList<Object>();
		if(((WrappedArray<BigDecimal>)buffer1.get(0)).size()>0)  {
			Object[] arr1 = (Object[]) ((WrappedArray<BigDecimal>)buffer1.get(0)).array();
			l1.addAll(Arrays.asList(arr1));
		}
		List<Object> l2 = new ArrayList<Object>();
		
		if(((WrappedArray<BigDecimal>)buffer2.get(0)).size()>0)  {
			Object[] arr2 = (Object[]) ((WrappedArray<BigDecimal>)buffer2.get(0)).array();
			l2.addAll(Arrays.asList(arr2));
		}
		l2.addAll(l1);
		buffer1.update(0, l2);

	}
	// Calculates the final result
	public BigDecimal evaluate(Row buffer) {
		Object[] obj = (Object[]) ((WrappedArray<BigDecimal>)buffer.get(0)).array();
		if(obj.length>0) {
			Arrays.sort(obj);
			return  (BigDecimal) obj[obj.length/2];
		} else {
			return null;
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
		spark.udf().register("Median", new Median());

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

