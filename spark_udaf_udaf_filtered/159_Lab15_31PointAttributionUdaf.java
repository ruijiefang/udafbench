//https://raw.githubusercontent.com/vpostrigan/study_java17/886d66b2ab65219ad89ece71de5264dc80321c23/spark/spark3_in_action__perrin_2021/src/main/java/spark_in_action2021/part3transform_data/Lab15_31PointAttributionUdaf.java
package spark_in_action2021.part3transform_data;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * архитектура UDAF: будет обрабатываться каждая запись,
 * а результат можно сохранить в буфере агрегации (на рабочих узлах).
 *
 * Буфер необязательно должен отображать структуру входных данных:
 * вы сами определяете его схему, и в нем можно сохронять и другие элементы
 *
 * Calculates loyalty points using a UDAF
 *
 * @author jgp
 */
public class Lab15_31PointAttributionUdaf extends UserDefinedAggregateFunction {
    private static Logger log = LoggerFactory.getLogger(Lab15_31PointAttributionUdaf.class);

    private static final long serialVersionUID = -66830400L;

    public static final int MAX_POINT_PER_ORDER = 3;

    /**
     * Describes the schema of input sent to the UDAF. Spark UDAFs can operate
     * on any number of columns. In our use case, we only need one field.
     */
    @Override
    public StructType inputSchema() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("_c0", DataTypes.IntegerType, true));
        return DataTypes.createStructType(inputFields);
    }

    /**
     * Describes the schema of UDAF buffer.
     */
    @Override
    public StructType bufferSchema() {
        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("sum", DataTypes.IntegerType, true));
        return DataTypes.createStructType(bufferFields);
    }

    /**
     * Datatype of the UDAF's output. (тип данных конечного результата)
     */
    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    /**
     * (результат не зависит от порядка - true)
     * Describes whether the UDAF is deterministic or not.
     * <p>
     * As Spark executes by splitting data, it processes the chunks separately
     * and combining them. If the UDAF logic is such that the result is
     * independent of the order in which data is processed and combined then
     * the UDAF is deterministic.
     */
    @Override
    public boolean deterministic() {
        return true;
    }

    /**
     * Initializes the buffer. This method can be called any number of times
     * of Spark during processing.
     * <p>
     * The contract should be that applying the merge function on two initial
     * buffers should just return the initial buffer itself, i.e.
     * `merge(initialBuffer, initialBuffer)` should equal `initialBuffer`.
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        log.trace("-> initialize() - buffer as {} row(s)", buffer.length());
        buffer.update(
                0, // column
                0); // value
        // You can repeat that for the number of columns you have in your buffer
    }

    /**
     * метод вызывается однократно для каждой входной записи.
     *
     * Updates the buffer with an input row. Validations on input should be
     * performed in this method.
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        log.trace("-> update(), input row has {} args", input.length());
        if (input.isNullAt(0)) {
            log.trace("Value passed is null.");
            return;
        }
        log.trace("-> update({}, {})", buffer.getInt(0), input.getInt(0));

        // Apply your business rule, could be in an external function/service.
        int initialValue = buffer.getInt(0); // начальное значение из буфера
        // вычисление количества баллов, заработанного этим заказом
        int inputValue = input.getInt(0);
        int outputValue = inputValue < MAX_POINT_PER_ORDER ? inputValue : MAX_POINT_PER_ORDER;
        // добавление вычисленных быллов к сумме ранее заработанных быллов
        outputValue += initialValue;

        log.trace("Value passed to update() is {}, this will grant {} points",
                inputValue,
                outputValue);
        // сохранение нового значения в столбце #0 буфера агрегации
        buffer.update(0, outputValue);
    }

    /**
     * Merges two aggregation buffers and stores the updated buffer values
     * back to buffer.
     */
    @Override
    public void merge(MutableAggregationBuffer buffer, Row row) {
        log.trace("-> merge({}, {})", buffer.getInt(0), row.getInt(0));
        buffer.update(0, buffer.getInt(0) + row.getInt(0));
    }

    /**
     * Calculates the final result of this UDAF based on the given aggregation
     * buffer.
     */
    @Override
    public Integer evaluate(Row row) {
        log.trace("-> evaluate({})", row.getInt(0));
        return row.getInt(0);
    }

}