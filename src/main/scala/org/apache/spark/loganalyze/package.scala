package org.apache.spark

package object loganalyze {

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
}