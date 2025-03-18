//https://raw.githubusercontent.com/kwlee0220/marmot.spark2/718f69c8e6a7e105aee709de2d78fbcea6ad7814/src/main/java/marmot/spark/type/ConvexHullUDAF.java
package marmot.spark.type;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Geometry;

import marmot.geo.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConvexHullUDAF extends UserDefinedAggregateFunction {
	private static final long serialVersionUID = 1L;

	@Override
	public StructType inputSchema() {
		return DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("geom", GeometryUDT.UDT, true)
		});
	}

	@Override
	public StructType bufferSchema() {
		return DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("geom", GeometryUDT.UDT, true)
		});
	}

	@Override
	public DataType dataType() {
		return GeometryUDT.UDT;
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		setBuffer(buffer, GeoClientUtils.EMPTY_POLYGON);
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		Geometry accum = fromBuffer(buffer);
		Geometry geom = (Geometry)input.get(0);
		if ( geom != null ) {
			accum = accum.union(geom).convexHull();
			setBuffer(buffer, accum);
		}
	}

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		Geometry accum = fromBuffer(buffer1);
		Geometry geom = fromBuffer(buffer2);
		accum = accum.union(geom);
		
		setBuffer(buffer1, accum.convexHull());
	}

	@Override
	public Object evaluate(Row buffer) {
		return fromBuffer(buffer);
	}

	private Geometry fromBuffer(MutableAggregationBuffer buffer) {
		Geometry geom = (Geometry)buffer.get(0);
		if ( geom == null ) {
			geom = GeoClientUtils.EMPTY_POLYGON;
		}
		return geom;
	}

	private Geometry fromBuffer(Row buffer) {
		Geometry geom = (Geometry)buffer.get(0);
		if ( geom == null ) {
			geom = GeoClientUtils.EMPTY_POLYGON;
		}
		return geom;
	}
	
	private void setBuffer(MutableAggregationBuffer buffer, Geometry poly) {
		buffer.update(0, poly);
	}
}
