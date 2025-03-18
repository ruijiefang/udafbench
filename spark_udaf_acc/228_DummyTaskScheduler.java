//https://raw.githubusercontent.com/plutolove/spark/04d85ab03c8017a115839ef1228142f42c6ca6bb/core/target/java/org/apache/spark/scheduler/DummyTaskScheduler.java
package org.apache.spark.scheduler;
public  class DummyTaskScheduler implements org.apache.spark.scheduler.TaskScheduler {
  // not preceding
  public   DummyTaskScheduler ()  { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> applicationAttemptId ()  { throw new RuntimeException(); }
  public  void cancelTasks (int stageId, boolean interruptThread)  { throw new RuntimeException(); }
  public  int defaultParallelism ()  { throw new RuntimeException(); }
  public  boolean executorHeartbeatReceived (java.lang.String execId, scala.Tuple2<java.lang.Object, scala.collection.Seq<org.apache.spark.util.AccumulatorV2<?, ?>>>[] accumUpdates, org.apache.spark.storage.BlockManagerId blockManagerId, scala.collection.mutable.Map<scala.Tuple2<java.lang.Object, java.lang.Object>, org.apache.spark.executor.ExecutorMetrics> executorMetrics)  { throw new RuntimeException(); }
  public  void executorLost (java.lang.String executorId, org.apache.spark.scheduler.ExecutorLossReason reason)  { throw new RuntimeException(); }
  // not preceding
  public  boolean initialized ()  { throw new RuntimeException(); }
  public  void killAllTaskAttempts (int stageId, boolean interruptThread, java.lang.String reason)  { throw new RuntimeException(); }
  public  boolean killTaskAttempt (long taskId, boolean interruptThread, java.lang.String reason)  { throw new RuntimeException(); }
  public  void notifyPartitionCompletion (int stageId, int partitionId)  { throw new RuntimeException(); }
  public  org.apache.spark.scheduler.Pool rootPool ()  { throw new RuntimeException(); }
  public  scala.Enumeration.Value schedulingMode ()  { throw new RuntimeException(); }
  public  void setDAGScheduler (org.apache.spark.scheduler.DAGScheduler dagScheduler)  { throw new RuntimeException(); }
  public  void start ()  { throw new RuntimeException(); }
  public  void stop ()  { throw new RuntimeException(); }
  public  void submitTasks (org.apache.spark.scheduler.TaskSet taskSet)  { throw new RuntimeException(); }
  public  void workerRemoved (java.lang.String workerId, java.lang.String host, java.lang.String message)  { throw new RuntimeException(); }
}
