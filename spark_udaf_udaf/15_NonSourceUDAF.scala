//https://raw.githubusercontent.com/teeyog/IQL/5b746a86007f038e842e5f2b529e4a5224a180a0/iql-engine/src/main/java/iql/engine/udf/NonSourceUDAF.scala
package iql.engine.udf

import org.apache.spark.SparkUtils
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

/**
  * Created by UFO on 12/25/2018.
  */
object NonSourceUDAF {
    def apply(className: String): UserDefinedAggregateFunction = {
        val clazz = Class.forName(className, true,  SparkUtils.getContextOrSparkClassLoader)
        SourceCodeCompiler.newInstance(clazz).asInstanceOf[UserDefinedAggregateFunction]
    }
}