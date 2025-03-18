//https://raw.githubusercontent.com/Hwyee/bigdata/b27cc156dc2f9cea8a2dae5cefe917e4a6b51eed/spark/src/main/java/cn/hwyee/sql/MyUdaf.java
package cn.hwyee.sql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * @author hwyee@foxmail.com
 * @version 1.0
 * @ClassName MyUdaf
 * @description 计算平均值
 * IN 输入
 * BUF 缓冲区 必须是公共类
 * OUT 输出
 * @date 2024/7/1
 * @since JDK 1.8
 */
public class MyUdaf extends Aggregator<Long,MyBuf,Long> {

    /**
     * zero:
     * 缓冲区初始化
     * @author hui
     * @version 1.0
     * @return cn.hwyee.sql.MyBuf
     * @date 2024/7/1 22:50
     */
    @Override
    public MyBuf zero() {
        return new MyBuf();
    }

    @Override
    public MyBuf reduce(MyBuf b, Long a) {
        b.setCount(b.getCount()+1);
        b.setSum(b.getSum()+a);
        return b;
    }

    /**
     * merge:
     * 合并缓冲区
     * @author hui
     * @version 1.0
     * @param b1
     * @param b2
     * @return cn.hwyee.sql.MyBuf
     * @date 2024/7/1 22:53
     */
    @Override
    public MyBuf merge(MyBuf b1, MyBuf b2) {
        b1.setSum(b1.getSum()+b2.getSum());
        b1.setCount(b1.getCount()+b2.getCount());
        return b1;
    }

    @Override
    public Long finish(MyBuf reduction) {
        return reduction.getSum()/reduction.getCount();
    }

    @Override
    public Encoder<MyBuf> bufferEncoder() {
        //自定义的类 用Encoders.bean()
        return Encoders.bean(MyBuf.class);
    }

    @Override
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }
}

