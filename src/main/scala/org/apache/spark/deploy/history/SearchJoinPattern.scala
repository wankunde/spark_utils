/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.EventLogInputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EVENT_LOG_DIR
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods.parse

/**
 * <pre>
 *mv ./conf/metrics.properties ./conf/metrics.properties_bak
 *mv ./conf/log4j.properties ./conf/log4j.properties_bak
 *
 *bin/spark-submit --master yarn --driver-memory 8G \
 *--executor-memory 4G \
 *--class org.apache.spark.deploy.history.SearchJoinPattern \
 *--conf spark.driver.memoryOverhead=0 \
 *--conf spark.driver.extraJavaOptions="" \
 *--conf spark.memory.offHeap.enabled=false \
 *--conf spark.memory.offHeap.size=0 \
 *--conf spark.executor.cores=4 \
 *--conf spark.executor.instances=90 \
 *--conf spark.executor.memory=4g \
 *--conf spark.executor.extraJavaOptions="" \
 *--conf spark.executor.memoryOverhead=0 \
 *--conf spark.yarn.queue=hdmi-staging \
 *~/wakun/spark_utils-1.0.jar
 *
 * 查看执行结果: yarn logs -applicationId  application_1622159252184_10033 | grep 'Find pattern join'
 *   </pre>
 */
object SearchJoinPattern extends Logging {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Search Join Pattern")
      .getOrCreate()
    val sc = spark.sparkContext
    val sparkConf = sc.conf

    import org.apache.spark.deploy.history.EventLogUtils._

    val util = SparkHadoopUtil.get
    //!!! Be careful, not HISTORY_LOG_DIR parameter
    val logDir = sparkConf.get(EVENT_LOG_DIR)
    val fsSystem = FileSystem.get(util.conf)

    val now = System.currentTimeMillis()

    Range(0, 4).foreach(dayBefore => {
      val logFiles = util.listFilesSorted(
        fsSystem,
        new Path(logDir),
        "application_",
        ".inprogress")
        .filter(fileStatus => {
          val mtime = fileStatus.getModificationTime
          mtime > (now - 86400000 * (dayBefore + 1)) && mtime < now - 86400000 * dayBefore
        })
        .map(_.getPath.toString)

      logInfo(s"Try to analyze ${logFiles.size} log files in ${logDir}")

      sc.hadoopFile[LongWritable, Text, EventLogInputFormat](logFiles.mkString(","))
        .map(_._2.toString)
        .filter(_.checkEventType(Set(
          "SparkListenerApplicationStart",
          "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate")))
        .foreachPartition { partitionIterator: Iterator[String] =>
          var appId: String = ""
          partitionIterator.foreach { log =>
            try {
              JsonProtocol.sparkEventFromJson(parse(log)) match {
                case SparkListenerSQLAdaptiveExecutionUpdate(_, desc, sparkPlanInfo) =>
                  if (log.contains("InsertIntoHadoopFsRelationCommand")) {
                    val idx = log.indexOf("InsertIntoHadoopFsRelationCommand")
                    val insertName: String = log.substring(idx, idx + 300)
                    SearchJoinPatternUtils.parsePlanInfo(sparkPlanInfo, appId, insertName)
                  }

                case SparkListenerApplicationStart(_, appIdOpt, _, _, _, _, _) if appIdOpt.isDefined =>
                  appId = appIdOpt.get

                case _ => // SKIP other events
              }
            } catch {
              case e: Exception =>
                logError(s"Fail to parse log appId = ${appId} log = ${log.substring(0, 100)}", e)
            }
          }
        }
    })

    spark.stop()
  }

}
