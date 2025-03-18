//https://raw.githubusercontent.com/cube2222/sparkschedulertest/a3445ab35f9afaac6d0f5466ef9b48f2402a742e/src/main/scala/ScalaniaTaskScheduler.scala
package org.apache.spark
/**
  * Created by jakub on 7/20/2016.
  */
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.scheduler.{DAGScheduler, Pool, TaskSet, _}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

class ScalaniaTaskScheduler extends ExternalClusterManager {
  override def canCreate(masterURL: String): Boolean = masterURL match {
    case "yarn" => {
      println("true!!!!!")
      true
    }
    case _ => {
      println("false!!!!!")
      false
    }
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskScheduler {override def applicationAttemptId(): Option[String] = Some("Hello world!!!")

      override def stop(): Unit = {}

      override def schedulingMode: SchedulingMode = SchedulingMode.FIFO

      override def defaultParallelism(): Int = 4

      override def submitTasks(taskSet: TaskSet): Unit = {}

      override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {}

      override def executorHeartbeatReceived(execId: String, accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])], blockManagerId: BlockManagerId): Boolean = true

      override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}

      override def rootPool: Pool = new Pool("My pool", SchedulingMode.FIFO, 10, 20)

      override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {}

      override def start(): Unit = {
        println("SCHEDULER STARTED **************************************")
      }
    }
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {

  }

  override def createSchedulerBackend(sc: SparkContext, masterURL: String, scheduler: TaskScheduler): SchedulerBackend = {
    return new SchedulerBackend {override def stop(): Unit = {}

      override def defaultParallelism(): Int = 4

      override def reviveOffers(): Unit = {}

      override def start(): Unit = {}
    }
  }
}
