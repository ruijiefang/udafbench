//https://raw.githubusercontent.com/NanTingPer/Learning-Notes/e9746ab2370f06fa28bf466b07fe42ddb1363cd5/codeor/Spark/src/main/java/SparkSQL/SQLUDAF_AvgAge.java
package SparkSQL;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

class SQLUDAF_AvgAge extends Aggregator
{
    @Override
//初始化缓冲区
    public Object zero()
    {
        return new SQLUDAF_UserDefClass(0L, 0L);
    }

    @Override
//聚合
    public Object reduce(Object b, Object a)
    {
        SQLUDAF_UserDefClass c = (SQLUDAF_UserDefClass) b;
        c.setCont(c.getCont() + 1);
        c.setSum(c.getSum() + (long) a);
        System.out.println("reduceA : " + a + "c.Cont And Sum: " + c.getCont() + " " + c.getSum());
        return c;
    }

    @Override
//缓冲区内聚合
    public Object merge(Object b1, Object b2)
    {
        SQLUDAF_UserDefClass b1_ = (SQLUDAF_UserDefClass) b1;
        SQLUDAF_UserDefClass b2_ = (SQLUDAF_UserDefClass) b2;
        b1_.setSum(b1_.getSum() + b2_.getSum());
        b1_.setCont(b1_.getCont() + b2_.getCont());
        return b1;
    }

    @Override
//最终
    public Object finish(Object reduction)
    {
        SQLUDAF_UserDefClass red = (SQLUDAF_UserDefClass) reduction;
        System.out.println("finish最终计算 : " + red.getSum() + " "  + red.getCont());
        return red.getSum() / red.getCont();
    }

    @Override
    public Encoder<SQLUDAF_UserDefClass> bufferEncoder()
    {
        return Encoders.bean(SQLUDAF_UserDefClass.class);
    }

    @Override
    public Encoder<Long> outputEncoder()
    {
        return Encoders.LONG();
    }
}

