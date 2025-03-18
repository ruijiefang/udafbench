//https://raw.githubusercontent.com/liudu2326526/liuduTest/5abddac527af8261273e101e619ae2d9780f1d56/spark/src/main/java/liudu/spark/java/udf/UdfUtils.java
package liudu.spark.java.udf;

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.RoaringBitmap;

public class UdfUtils {

  static final List<Long> LIKE_BINS = ImmutableList
      .of(0L, 3L, 5L, 10L, 15L, 30L, 50L, 70L, 100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L, 900L,
          1000L, 1500L, 2000L, 2500L, 3000L, 3500L, 4000L, 4500L, 5000L, 6000L, 7000L, 8000L,
          10000L, 15000L, 20000L, 30000L, 50000L, 100000L);

  public static void register(String name, SparkSession ss) {

    switch (name) {
      case "like_cum_rk":
        ss.udf().register(name, new LikeCumRank(), DataTypes.IntegerType);
        break;
      case "get_max_time_data":
        ss.udf().register(name, new GetMaxTimeData());
        break;
      case "rbm_agg":
        ss.udf()
            .register(name, functions.udaf(new StringToRoaringBitmapUnion(), Encoders.STRING()));
        break;
      case "rbm_count":
        ss.udf().register(name, new RoaringBitmapCount(), DataTypes.LongType);
        break;
      case "rbm_or":
        ss.udf().register(name, new RoaringBitmapOr(), DataTypes.BinaryType);
        break;
      case "rbm_and":
        ss.udf().register(name, new RoaringBitmapAnd(), DataTypes.BinaryType);
        break;
    }
  }

  static class RoaringBitmapCount implements UDF1<byte[], Long> {

    @Override
    public Long call(byte[] bytes) throws Exception {
      RoaringBitmap rbm = new RoaringBitmap();
      rbm.deserialize(ByteBuffer.wrap(bytes));
      return rbm.getLongCardinality();
    }
  }

  static class RoaringBitmapOr implements UDF2<byte[], byte[], byte[]> {

    @Override
    public byte[] call(byte[] array1, byte[] array2) throws Exception {

      RoaringBitmap r1 = new RoaringBitmap();
      RoaringBitmap r2 = new RoaringBitmap();

      r1.deserialize(ByteBuffer.wrap(array1));
      r2.deserialize(ByteBuffer.wrap(array2));

      RoaringBitmap or = RoaringBitmap.or(r1, r2);

      RoaringBitmap r3 = new RoaringBitmap();
      byte[] array = new byte[or.serializedSizeInBytes()];
      r3.serialize(ByteBuffer.wrap(array));

      return array;
    }

  }

  static class RoaringBitmapAnd implements UDF2<byte[], byte[], byte[]> {

    @Override
    public byte[] call(byte[] array1, byte[] array2) throws Exception {

      RoaringBitmap r1 = new RoaringBitmap();
      RoaringBitmap r2 = new RoaringBitmap();

      r1.deserialize(ByteBuffer.wrap(array1));
      r2.deserialize(ByteBuffer.wrap(array2));

      RoaringBitmap and = RoaringBitmap.and(r1, r2);

      RoaringBitmap r3 = new RoaringBitmap();
      byte[] array = new byte[and.serializedSizeInBytes()];
      r3.serialize(ByteBuffer.wrap(array));

      return array;
    }

  }

  static class StringToRoaringBitmapUnion extends Aggregator<String, RoaringBitmap, byte[]> {

    @Override
    public RoaringBitmap zero() {
      return new RoaringBitmap();
    }

    @Override
    public RoaringBitmap reduce(RoaringBitmap b, String a) {
      b.add(a.hashCode());
      return b;
    }

    @Override
    public RoaringBitmap merge(RoaringBitmap b1, RoaringBitmap b2) {
      b1.or(b2);
      return b1;
    }

    @Override
    public byte[] finish(RoaringBitmap reduction) {
      byte[] array = new byte[reduction.serializedSizeInBytes()];
      reduction.serialize(ByteBuffer.wrap(array));

      return array;
    }

    @Override
    public Encoder<RoaringBitmap> bufferEncoder() {
      return Encoders.javaSerialization(RoaringBitmap.class);
    }

    @Override
    public Encoder<byte[]> outputEncoder() {
      return Encoders.BINARY();
    }
  }

  static class LikeCumRank implements UDF1<Long, Integer> {

    @Override
    public Integer call(Long aLong) throws Exception {
      for (int i = 0; i < LIKE_BINS.size(); i++) {
        if (aLong <= LIKE_BINS.get(i)) {
          return i;
        }
      }
      return -1;
    }
  }

  static class GetMaxTimeData extends UserDefinedAggregateFunction {

    @Override
    public StructType inputSchema() {
      return DataTypes.createStructType(Collections.singletonList(
          new StructField("data", DataTypes.createArrayType(DataTypes.StringType), true,
              Metadata.empty())
          )
      );
    }

    @Override
    public StructType bufferSchema() {
      return DataTypes.createStructType(Collections.singletonList(
          new StructField("data", DataTypes.createArrayType(DataTypes.StringType), true,
              Metadata.empty())
          )
      );
    }

    @Override
    public DataType dataType() {
      return DataTypes.StringType;
    }

    @Override
    public boolean deterministic() {
      return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
      buffer.update(0, Arrays.asList(null, "0"));
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
      if (input.isNullAt(0)) {
        return;
      }
      List<String> bufferList = buffer.getList(0);
      List<String> inputList = input.getList(0);
      if (inputList.get(1) != null && inputList.get(0) != null
          && bufferList.get(1).compareTo(inputList.get(1)) < 0) {
        buffer.update(0, inputList);
      }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
      if (buffer2.isNullAt(0)) {
        return;
      }
      List<String> buffer1List = buffer1.getList(0);
      List<String> buffer2List = buffer2.getList(0);
      if (buffer1List.get(1).compareTo(buffer2List.get(1)) < 0) {
        buffer1.update(0, buffer2List);
      }
    }

    @Override
    public Object evaluate(Row buffer) {
      return buffer.getList(0).get(0);
    }
  }
}
