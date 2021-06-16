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
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{EventLogInputFormat, InputSplit}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EVENT_LOG_DIR
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods.parse

/**
 * <pre>
 *mv ./conf/metrics.properties ./conf/metrics.properties_bak
 *mv ./conf/log4j.properties ./conf/log4j.properties_bak
 *
 *bin/spark-submit --master yarn --driver-memory 1G \
 *--executor-memory 4G \
 *--class org.apache.spark.deploy.history.SearchJoinPattern \
 *--conf spark.driver.memoryOverhead=0 \
 *--conf spark.driver.extraJavaOptions="" \
 *--conf spark.memory.offHeap.enabled=false \
 *--conf spark.memory.offHeap.size=0 \
 *--conf spark.executor.cores=4 \
 *--conf spark.executor.instances=30 \
 *--conf spark.executor.memory=2g \
 *--conf spark.executor.extraJavaOptions="" \
 *--conf spark.executor.memoryOverhead=0 \
 *--conf spark.yarn.queue=hdmi-kudu \
 *~/wakun/spark_utils-1.0.jar
 *
 * 查看执行结果: yarn logs -applicationId  application_1622159252184_9981 | more
 *   </pre>
 */
object SearchJoinPatternNewApi extends Logging {

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

    sc.newAPIHadoopFile[LongWritable, Text, EventLogInputFormat](logFiles.mkString(","))
      .asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      .mapPartitionsWithInputSplit((inputSplit: InputSplit, iterator: Iterator[(LongWritable, Text)]) => {
        val fileName = inputSplit.asInstanceOf[FileSplit].getPath.toString
        iterator.map(_._2.toString)
          .filter(_.checkEventType(Set("org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate")))
          .map { log =>
            try {
              JsonProtocol.sparkEventFromJson(parse(log)) match {
                case SparkListenerSQLAdaptiveExecutionUpdate(executionId, desc, sparkPlanInfo) =>
                  parsePlanInfo(sparkPlanInfo, fileName)

                case _ => // SKIP other events
              }
            } catch {
              case e: Exception =>
                logError(s"Fail to parse log fileName = ${fileName} log = ${log.substring(0, 100)}", e)
            }
          }
      })

    spark.stop()
  }

  def parsePlanInfo(plan: SparkPlanInfo, appId: String): Unit = {
    if (plan.nodeName == "SortMergeJoin") {
      matchJoinPattern(firstExchange(plan.children(0)), firstExchange(plan.children(1))) match {
        case Some((left, right)) =>
          println(s"Find pattern join in appId: $appId")
        case _ =>
      }
    }
    plan.children.map(parsePlanInfo(_, appId))
  }

  /**
   * must return a node
   * @param node
   * @return
   */
  def firstExchange(node: SparkPlanInfo): Option[SparkPlanInfo] = {
    if (node.nodeName == "Exchange" || node.nodeName == "CustomShuffleReader") {
      Some(node)
    } else if (node.nodeName.contains("Join")) {
      None
    } else {
      node.children.map(firstExchange(_)).find(_.isDefined).getOrElse(None)
    }
  }

  def matchJoinPattern(node1Opt: Option[SparkPlanInfo],
                       node2Opt: Option[SparkPlanInfo]): Option[(SparkPlanInfo, SparkPlanInfo)] = {
    if (node1Opt == None || node2Opt == None) {
      None
    } else {
      val node1 = node1Opt.get
      val node2 = node2Opt.get
      if (node1.nodeName == "Exchange" && node2.simpleString == "CustomShuffleReader coalesced") {
        Some((node1, node2))
      } else if (node2.nodeName == "Exchange" && node1.simpleString == "CustomShuffleReader coalesced") {
        Some((node2, node1))
      } else {
        None
      }
    }
  }
}
