//https://raw.githubusercontent.com/jaceklaskowski/spark-workshop/a6fd3ee00f1a413c5efa49a8f04e72947f411234/solutions/spark-external-cluster-manager/src/main/scala/ScalaniaClusterManager.scala
package org.apache.spark

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

class ScalaniaClusterManager extends ExternalClusterManager {

  val masterUrl = "scalania"

  override def canCreate(masterURL: String): Boolean = {
    println("[scalania] canCreate")
    masterURL.toLowerCase == masterUrl
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskScheduler {

      override def applicationAttemptId(): Option[String] = {
        Some(s"$masterUrl-attempt-id")
      }

      override def stop(): Unit = {
        println(s"[$masterUrl] stop")
      }

      override def schedulingMode: SchedulingMode = SchedulingMode.FIFO

      override def defaultParallelism(): Int = ???

      override def submitTasks(taskSet: TaskSet): Unit = ???

      override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = ???

      override def executorHeartbeatReceived(execId: String, accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])], blockManagerId: BlockManagerId): Boolean = ???

      override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = ???

      override def rootPool: Pool = ???

      override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
        println(s"[scalania] dagScheduler: $dagScheduler")
      }

      override def start(): Unit = {
        println("[scalania] start")
      }
    }
  }

  override def createSchedulerBackend(sc: SparkContext, masterURL: String, scheduler: TaskScheduler): SchedulerBackend = {
    new SchedulerBackend {override def stop(): Unit = ???

      override def defaultParallelism(): Int = ???

      override def reviveOffers(): Unit = ???

      override def start(): Unit = ???
    }
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    println("[scalania] initialize")
  }
}
