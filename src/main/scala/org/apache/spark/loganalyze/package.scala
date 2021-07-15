package org.apache.spark

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlanInfo

package object loganalyze extends Logging {

  implicit class JsonEventLog(event: String) {
    val pattern = s"""\\{"Event":"(.*?)"[\\s\\S]*""".r
    val pattern(eventType) = event

    def checkEventType(eventTypes: Set[String]): Boolean = {
      eventTypes.isEmpty || eventTypes.contains(eventType)
    }
  }

  val skippedEventType: Set[String] =
    Set("SparkListenerLogStart",
      "SparkListenerApplicationStart",
      "SparkListenerApplicationEnd",
      "org.apache.spark.scheduler.SparkListenerMiscellaneousProcessAdded",
      "SparkListenerResourceProfileAdded",
      "SparkListenerBlockManagerAdded",
      "SparkListenerEnvironmentUpdate",
      "SparkListenerExecutorAdded",
      // TODO: Stage事件后续可用于DAG Schedule 资源调度优化
      "SparkListenerStageSubmitted",
      "SparkListenerStageCompleted",
      "SparkListenerTaskStart",
      "SparkListenerTaskEnd")

  implicit def planToQueue(plan: SparkPlanInfo): mutable.Queue[SparkPlanInfo] = mutable.Queue[SparkPlanInfo](plan)

  def transformPlanInfo(plans: mutable.Queue[SparkPlanInfo], func: SparkPlanInfo => Unit): Unit = {
    while (!plans.isEmpty) {
      val plan = plans.dequeue()
      func(plan)
      plans ++= plan.children
    }
  }

  def hashPartitionNumberOpt(plan: SparkPlanInfo): Option[Long] = {
    if (plan.simpleString.startsWith("Exchange hashpartitioning")) {
      Some(extractHashPartitioning(plan.simpleString))
    } else if (plan.children.size > 0) {
      hashPartitionNumberOpt(plan.children(0))
    } else {
      None
    }
  }

  def extractHashPartitioning(nodeName: String): Long = {
    try {
      val j = nodeName.lastIndexOf(')')
      var i = j
      while (i > 0 && nodeName(i) != ',') {
        i = i - 1
      }
      nodeName.substring(i+1, j).trim.toLong
    } catch {
      case e: Exception =>
        logError(s"Failed to parse HashPartitioning ${nodeName}")
        0
    }
  }
}