//https://raw.githubusercontent.com/jiasenwqbr/bigdata-practise/07c30f5c826c132136eb5098186cc6f587be1d51/spark/src/main/java/com/jason/spark/sql/Test05_UDAF.java
package com.jason.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import static org.apache.spark.sql.functions.udaf;
import java.io.Serializable;
/**
 * @Description:
 * @author: 贾森
 * @date: 2024年10月30日 11:08
 */
public class Test05_UDAF {
    public static void main(String[] args) {
        //1. 创建配置对象
        SparkConf conf = new SparkConf().setAppName("sparksql").setMaster("local[*]");

        //2. 获取sparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        //3. 编写代码
        Dataset<Row> lineRDD = spark.read().json("datas/user.json");

        lineRDD.createOrReplaceTempView("user");
        // 注册需要导入依赖 import static org.apache.spark.sql.functions.udaf;
        spark.udf().register("avgAge",udaf(new MyAvg(),Encoders.LONG()));

        spark.sql("select avgAge(age) newAge from user").show();

        //4. 关闭sparkSession
        spark.close();

    }

    public static class Buffer implements Serializable {
        private Long sum;
        private Long count;

        public Buffer() {
        }

        public Buffer(Long sum, Long count) {
            this.sum = sum;
            this.count = count;
        }

        public Long getSum() {
            return sum;
        }

        public void setSum(Long sum) {
            this.sum = sum;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }
    }
    public static class MyAvg extends Aggregator<Long,Buffer,Double>{

        @Override
        public Buffer zero() {
            return new Buffer(0L,0L);
        }

        @Override
        public Buffer reduce(Buffer b, Long a) {
            b.setCount(b.getCount()+1);
            b.setSum(b.getSum()+a);
            return b;
        }

        @Override
        public Buffer merge(Buffer b1, Buffer b2) {
            b1.setSum(b1.getSum() + b2.getSum());
            b1.setCount(b1.getCount() + b2.getCount());

            return b1;
        }

        @Override
        public Double finish(Buffer reduction) {
            return reduction.getSum().doubleValue() / reduction.getCount();
        }

        @Override
        public Encoder<Buffer> bufferEncoder() {
            // 可以用kryo进行优化
            return Encoders.kryo(Buffer.class);
        }

        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }



}

