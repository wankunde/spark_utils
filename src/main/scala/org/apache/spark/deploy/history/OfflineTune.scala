package org.apache.spark.deploy.history

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.EventLogInputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EVENT_LOG_DIR
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui._
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable

/**
 * bin/spark-submit --master yarn --driver-memory 1G --executor-memory 1G --num-executors 2 --class org.apache.spark.deploy.history.OfflineTuning ~/.m2/repository/com/wankun/spark_utils/1.0/spark_utils-1.0.jar
 */
object OfflineTuning extends Logging {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Job Offline Tuning")
      .getOrCreate()
    val sc = spark.sparkContext
    val sparkConf = sc.conf

    val smallLimit = sparkConf.getLong("spark.tuning.mergeInsert.avgSmallSize", 15 * 1024 * 1024)
    val mergeLimit = sparkConf.getLong("spark.tuning.mergeInsert.avgTargetSize", 128 * 1024 * 1024)
    val compactFactor = sparkConf.getLong("spark.tuning.mergeInsert.mergeFactor", 2)

    import org.apache.spark.deploy.history.EventLogUtils._

    val util = SparkHadoopUtil.get
    //!!! Be careful, not HISTORY_LOG_DIR parameter
    val logDir = sparkConf.get(EVENT_LOG_DIR)
    val fsSystem = FileSystem.get(util.conf)

    val now = System.currentTimeMillis()

    val logFiles = util.listFilesSorted(
      fsSystem,
      new Path(logDir),
      "application_",
      ".inprogress")
      .filter(fileStatus => {
        val mtime = fileStatus.getModificationTime
        mtime > now - 86400000 && mtime < now - 43200000
      })
      .map(_.getPath.toString)


    logInfo(s"Try to analyze ${logFiles.size} log files in ${logDir}")

    sc.hadoopFile[LongWritable, Text, EventLogInputFormat](logFiles.mkString(","))
      .map(_._2.toString)
      .filter(!_.checkEventType(skippedEventType))
      .foreachPartition { partitionIterator: Iterator[String] =>
        val insertRelationExecutions = mutable.HashMap[Long, InsertRelationExecutionData]()
        partitionIterator.foreach { log =>
          try {
            JsonProtocol.sparkEventFromJson(parse(log)) match {
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

                  val suggestNumFiles = numOutputBytes / mergeLimit + 1
                  //                  if (numOutputBytes / numFiles < smallLimit &&
                  //                    suggestNumFiles * compactFactor < numFiles) {
                  val pattern = "[\\s\\S]*\\nDatabase: (.*)\nTable: (.*)\n[\\s\\S]*".r
                  val pattern(database, table) = executionData.simpleString

                  logInfo(
                    s"""number of written files: ${numFiles}
                       |written output: ${numOutputBytes}
                       |number of output rows: ${numOutputRows}
                       |number of dynamic part: ${numParts}
                       |
                       |database: $database, table: $table should merge output to $suggestNumFiles
                       |""".stripMargin)
                  //                  }
                }

              case _ => // SKIP other events
            }
          } catch {
            case e: Exception =>
              logError(s"Fail to parse log:${log}")
          }
        }

      }

    spark.stop()
  }
}
