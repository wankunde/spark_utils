package org.apache.spark.deploy.history

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui._
import org.apache.spark.util.{JsonProtocol, Utils}
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.io.Source

/**
 * bin/spark-submit --master yarn --driver-memory 1G --executor-memory 1G --num-executors 2 --class org.apache.spark.deploy.history.LogReader ~/.m2/repository/com/wankun/spark_utils/1.0/spark_utils-1.0.jar
 */
object LogReader extends Logging {

  import org.apache.spark.deploy.history.EventLogUtils._

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val logDir = "/directory"
    val logFileName = "application_1621217761643_0014.inprogress"
    val logPath = new Path(logDir, logFileName)
    val fs = logPath.getFileSystem(conf)
    Utils.tryWithResource(EventLogFileReader.openEventLog(logPath, fs)) { in =>
      val insertRelationExecutions = mutable.HashMap[Long, InsertRelationExecutionData]()
      val events = Source.fromInputStream(in).getLines()
        .filterNot(_.checkEventType(skippedEventType)).toList
      events.map(line => JsonProtocol.sparkEventFromJson(parse(line)))
        .collect {
          case e: SparkListenerSQLExecutionStart
            if e.sparkPlanInfo.nodeName == "Execute InsertIntoHadoopFsRelationCommand" =>
            val (executionId, description, planInfo) = (e.executionId, e.description, e.sparkPlanInfo)
            val metrics = planInfo.metrics.map(m => OperationMetric(m.accumulatorId, 0L, m.name))
            val operationMetrics = OperationMetrics(metrics)
            val simpleString = planInfo.simpleString
            insertRelationExecutions(executionId) =
              InsertRelationExecutionData(executionId, description, simpleString, operationMetrics)

          case SparkListenerDriverAccumUpdates(executionId, accumUpdates)
            if insertRelationExecutions.contains(executionId) =>
            val metrics = insertRelationExecutions(executionId).metrics
            accumUpdates.foreach(kv => metrics.update(kv._1, kv._2))

          case SparkListenerSQLAdaptiveExecutionUpdate(executionId, _, sparkPlanInfo)
            if sparkPlanInfo.nodeName == "Execute InsertIntoHadoopFsRelationCommand" =>
            insertRelationExecutions.get(executionId).map(executionData => {
              sparkPlanInfo.metrics.map(m => {
                val metric = OperationMetric(m.accumulatorId, 0L, m.name)
                executionData.metrics.getOrElseUpdate(metric)
              })
            })

          case SparkListenerSQLExecutionEnd(executionId, _) =>
            val executionDataOpt = insertRelationExecutions.remove(executionId)
            if (executionDataOpt.isDefined) {
              val executionData = executionDataOpt.get
              val metrics = executionData.metrics

              val numFiles = metrics.getByName("number of written files").get.value
              val numOutputBytes = metrics.getByName("written output").get.value
              val numOutputRows = metrics.getByName("number of output rows").get.value
              val numParts = metrics.getByName("number of dynamic part").get.value


              val samllLimit = 15 * 1024 * 1024
              val mergeLimit = 128 * 1024 * 1024
              val compactFactor = 2
              if (numOutputBytes / numFiles < samllLimit) {
                val suggestNumFiles = numOutputBytes / mergeLimit + 1
                if (suggestNumFiles * compactFactor < numFiles) {
                  val pattern = "[\\s\\S]*\\nDatabase: (.*)\nTable: (.*)\n[\\s\\S]*".r
                  val pattern(database, table) = executionData.simpleString
                  // DEBUG Message
                  println(
                    s"""number of written files: ${numFiles}
                       |written output: ${numOutputBytes}
                       |number of output rows: ${numOutputRows}
                       |number of dynamic part: ${numParts}
                       |""".stripMargin)
                  println(s"database: $database, table: $table should merge output to $suggestNumFiles")
                }
              }
            }
        }

    }
  }

}
