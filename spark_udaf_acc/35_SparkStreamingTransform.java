//https://raw.githubusercontent.com/prashant887/SparkJava/c71a42a69801cce8a8d95673a92a4ea4896250b8/src/main/java/common/SparkStreamingTransform.java
package common;

import config.SparkJobSettings;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;
import util.CollectorsMetadata;
import util.ExtractedMetricsParquetWriter;
import util.ParquetInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkStreamingTransform {

    public static class ToParquet implements FlatMapFunction<Iterator<Tuple2<String, String>>, Tuple2<String,Long>> {

        @Override
        public Iterator<Tuple2<String,Long>> call(Iterator<Tuple2<String, String>> message) throws Exception {
            System.out.println("TO Parquet :"+message);
           List<Tuple2<String,Long>> results = new ArrayList<Tuple2<String,Long>>();
            while (message.hasNext()) {
                Tuple2<String, String> messageOne = message.next();
                long extractStartTime = System.nanoTime();
                System.out.println(messageOne._2+" "+extractStartTime);
                results.add(new Tuple2<>(messageOne._2,extractStartTime));

            }
            return  results.iterator();
        }
    }

    public static class LoadParquetToDatabase implements VoidFunction<JavaRDD<Tuple2<String,Long>>> {

        @Override
        public void call(JavaRDD<Tuple2<String, Long>> tuple2JavaRDD) throws Exception {
            List<Tuple2<String,Long>> parquetInfos = tuple2JavaRDD.collect();

            for(Tuple2<String,Long> m:parquetInfos){
                System.out.println("Parquet Info :"+m._1+" "+m._2);
            }

        }

        }


    }
