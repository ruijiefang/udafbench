//https://raw.githubusercontent.com/cute5051/SparkTests/b1f25907fc425913c57d0716bda2e2af12e680c4/src/java/LogCollector.java
import model.SparkMetricModel;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.collection.Iterator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class LogCollector extends SparkListener {
    private static final long serialVersionUID = 1L;
    SparkSession session;
    List<SparkMetricModel> fullLogStageCompleted = new ArrayList<>();

    public LogCollector(SparkSession session) {
        this.session = session;
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        StageInfo stageInfo = stageCompleted.stageInfo();
        System.out.printf("****************** StageId: %s completed. totaltime: %s seconds, with %s tasks ******************%n", stageInfo.stageId(), stageInfo.taskMetrics().executorRunTime() / 1000, stageInfo.numTasks());
        long jvmGCTime = stageInfo.taskMetrics().jvmGCTime();
        long executorRunTime = stageInfo.taskMetrics().executorRunTime();
        double prediction_1 = (double) jvmGCTime / executorRunTime * 100; // % of time spent in GC and executor run time

        long bytesRead = stageInfo.taskMetrics().inputMetrics().bytesRead();
        long bytesWritten = stageInfo.taskMetrics().outputMetrics().bytesWritten();
        long shuffleBytesRead = stageInfo.taskMetrics().shuffleReadMetrics().remoteBytesRead();
        long shuffleLocalBytesRead = stageInfo.taskMetrics().shuffleReadMetrics().localBytesRead();
        long shuffleRemoteBytesReadToDisk = stageInfo.taskMetrics().shuffleReadMetrics().remoteBytesReadToDisk();
        long shuffleBytesWritten = stageInfo.taskMetrics().shuffleWriteMetrics().bytesWritten();
        long resultSize = stageInfo.taskMetrics().resultSize();
        long peakExecutionMemory = stageInfo.taskMetrics().peakExecutionMemory();
        long sumBytes = bytesRead + bytesWritten + shuffleBytesRead + shuffleLocalBytesRead +
                shuffleRemoteBytesReadToDisk + shuffleBytesWritten + resultSize + peakExecutionMemory;
        double prediction_2 = sumBytes / 1024D / 1024D;

        long diskBytesSpilled = stageInfo.taskMetrics().diskBytesSpilled();
        long memoryBytesSpilled = stageInfo.taskMetrics().memoryBytesSpilled();
        double prediction_3 = (double) diskBytesSpilled / memoryBytesSpilled / 1024D / 1024D / 2;

        System.out.printf("Prediction_1: %s. It's time spent in GC in percents %n", prediction_1);
        System.out.printf("Prediction_2: %smb. SumBytes in MB%n", prediction_2);
        System.out.printf("Prediction_3: %smb. Spill bytes in MB / 2%n", prediction_3);

        SparkMetricModel sparkMetricModel = new SparkMetricModel();
        sparkMetricModel.setStageId(stageInfo.stageId());
        Iterator<AccumulatorV2<?, ?>> it_tasks = stageInfo.taskMetrics().accumulators().iterator();
        while (it_tasks.hasNext()) {
            AccumulatorV2<?, ?> next = it_tasks.next();
            String key = next.name().get().replace("internal.metrics.", "").replace(".", "");
            Object value = next.value();
            for (Method method : SparkMetricModel.class.getMethods()) {
                if (method.getParameterCount() != 0 && method.getName().toLowerCase().endsWith(key.toLowerCase())) {
                    try {
                        System.out.printf("Вызвал method: %s, передал в него value: %s. Key был таким: %s%n", method.getName(), value, key);
                        method.invoke(sparkMetricModel, value);
                    } catch (IllegalAccessException e) {
                        System.out.printf("Словил IllegalAccessException. Method: %s%n key: %s%n value: %s%n", method.getName(), key, value);
                    } catch (InvocationTargetException e) {
                        System.out.printf("Словил InvocationTargetException. Method: %s%n key: %s%n value: %s%n", method.getName(), key, value);
                    } catch (IllegalArgumentException e) {
                        System.out.printf("Словил IllegalArgumentException. Method: %s%n key: %s%n value: %s%n", method.getName(), key, value);
                    }
                }
            }
        }
        fullLogStageCompleted.add(sparkMetricModel);
        System.out.println("****************** StageId: " + stageInfo.stageId() + " block end ******************");
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        System.out.println("\n application end ***************");
        System.out.println(fullLogStageCompleted);

//        try (PrintWriter pw = new PrintWriter(new FileWriter(new File("C:\\Users\\mouse\\IdeaProjects\\test.csv")))) {
//            pw.println(SparkMetricModel.toFieldNames());
//            for (SparkMetricModel smm : fullLogStageCompleted) {
//                pw.println(smm.toValueInObj());
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }
}