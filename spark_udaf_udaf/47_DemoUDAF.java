//https://raw.githubusercontent.com/its-ck/Spark-Examples/216a061f56c147a80e02198de09a8bd1395fd869/DemoUDAF.java
package spark.innoeye;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DemoUDAF extends UserDefinedAggregateFunction {

	@Override
	public StructType bufferSchema() {
		
		List<StructField> bf = new ArrayList<>();
	    bf.add(DataTypes.createStructField("marks", DataTypes.IntegerType, true));
	    return DataTypes.createStructType(bf);
		
	}

	@Override
	public DataType dataType() {
		return DataTypes.IntegerType;
	}

	@Override
	public boolean deterministic() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object evaluate(Row buff) {
		if (buff.isNullAt(0)) {
            return null;
        } else {
            return buff.getInt(2);
        }
		
	}

	@Override
	public void initialize(MutableAggregationBuffer buff) {
		System.out.println("Entered into the UDAF");
		buff.update(0, null);
	}

	@Override
	public StructType inputSchema() {
		
		List<StructField> sf = new ArrayList<>();
		sf.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		sf.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		sf.add(DataTypes.createStructField("marks", DataTypes.IntegerType, true));
		
		return DataTypes.createStructType(sf);
	}

	@Override
	public void merge(MutableAggregationBuffer oldBuffer, Row newBuffer) {
		if(!oldBuffer.isNullAt(0))
		{
			int oldValue = newBuffer.getInt(0);
			int newValue = newBuffer.getInt(2);
			oldBuffer.update(0, oldValue+newValue);
			
		}
		else
		{
			oldBuffer.update(0, newBuffer.getInt(2));				
		}

	}

	@Override
	public void update(MutableAggregationBuffer buff, Row input) {
		System.out.println("Inside the update method");
		if(!buff.isNullAt(0))
		{
			int oldValue = buff.getInt(0);
			int newValue = input.getInt(2);
			buff.update(0, oldValue+newValue);
			
		}
		else
		{
			buff.update(0, input.getInt(2));				
		}
	}

}
