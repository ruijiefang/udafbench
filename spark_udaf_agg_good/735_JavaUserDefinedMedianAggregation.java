//https://raw.githubusercontent.com/MukulK/test/5e28a78744eef1e117e8adcd9dea8604514ae387/JavaUserDefinedMedianAggregation.java
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
// $example off:untyped_custom_aggregation$
import org.apache.spark.sql.expressions.Aggregator;

public class JavaUserDefinedMedianAggregation {

	// $example on:untyped_custom_aggregation$
	public static class CollectionObject implements Serializable  {
		private List<BigDecimal> list ;

		// Constructors, getters, setters...
		// $example off:typed_custom_aggregation$
		public CollectionObject() {
			if(this.list == null) {
				this.list=new ArrayList<BigDecimal>();
			}
		}

		public void add(BigDecimal data) {
			this.list.add(data);

		}

		public void addAll(CollectionObject b2) {
			this.list.addAll(b2.getList());

		}

		

		public List<BigDecimal> getList() {
			return list;
		}

		public void setList(List<BigDecimal> list) {
			this.list = list;
		}

		public BigDecimal getMedian() {
			Collections.sort(list);
			if(list.size()>0) {
				return list.get(list.size()/2);
			} else {
				return null;
			}
		}


		@Override
		public String toString() {
			return this.list.toString();
		}
		// $example on:typed_custom_aggregation$
	}

	public static class Median extends Aggregator<BigDecimal, CollectionObject, BigDecimal> {
		// A zero value for this aggregation. Should satisfy the property that any b + zero = b
		public CollectionObject zero() {
			return new CollectionObject();
		}
		// Combine two values to produce a new value. For performance, the function may modify `buffer`
		// and return it instead of constructing a new object
		public CollectionObject reduce(CollectionObject buffer, BigDecimal data) {
			if(data != null) {
				buffer.add(data);
			}
			return buffer;
		}
		// Merge two intermediate values
		public CollectionObject merge(CollectionObject b1, CollectionObject b2) {
			b1.addAll(b2);
			return b1;
		}
		// Transform the output of the reduction
		public BigDecimal finish(CollectionObject reduction) {
			return reduction.getMedian();
		}
		// Specifies the Encoder for the intermediate value type
		public Encoder<CollectionObject> bufferEncoder() {
			return Encoders.bean(CollectionObject.class);
		}
		// Specifies the Encoder for the final output value type
		public Encoder<BigDecimal> outputEncoder() {
			return Encoders.DECIMAL();
		}
	}
	// $example off:untyped_custom_aggregation$

	public static void main(String[] args) {
		System.setProperty("HADOOP_HOME", "C:\\Users\\Mukul-CS\\Downloads\\spark-3.0.0-bin-hadoop3.2\\spark-3.0.0-bin-hadoop3.2");
		System.setProperty("hadoop.home.dir", "C:\\Users\\Mukul-CS\\Downloads\\spark-3.0.0-bin-hadoop3.2\\spark-3.0.0-bin-hadoop3.2");
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL user-defined DataFrames aggregation example")
				.config("spark.master", "local")
				.getOrCreate();

		// $example on:untyped_custom_aggregation$
		// Register the function to access it
		spark.udf().register("median", functions.udaf(new Median(), Encoders.DECIMAL()));

		Dataset<Row> df = spark.read().json("src\\employees.json");
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

		Dataset<Row> result = spark.sql("SELECT median(salary) as average_salary FROM employees");
		result.show();
		// +--------------+
		// |average_salary|
		// +--------------+
		// |        3750.0|
		// +--------------+
		// $example off:untyped_custom_aggregation$

		spark.stop();
	}
}