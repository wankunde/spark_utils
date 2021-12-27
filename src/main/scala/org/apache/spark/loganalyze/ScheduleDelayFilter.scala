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

package org.apache.spark.loganalyze

import org.apache.spark.Success
import org.apache.spark.loganalyze.AnalyzeBase.appId
import org.apache.spark.scheduler.SparkListenerTaskEnd

/**
 * 修改 AnalyzeBase 开启Application扫描过滤条件 .filter(p => p.getPath.toString.contains("1636603355091_20415"))
 * rm -rf spark_utils-1.0-bin.tar.gz spark_utils-1.0
 * rz
 * tar -zxvf spark_utils-1.0-bin.tar.gz
 * spark_utils-1.0/bin/spark_utils.sh custom_class org.apache.spark.loganalyze.ScheduleDelayFilter
 * spark_utils-1.0/bin/spark_utils.sh collect_result
 */
object ScheduleDelayFilter extends AnalyzeBase {

  def main(args: Array[String]): Unit = {

    sparkAnalyze(
      appName = "Schedule Delay Filter",
//      filePath =
//        "/Users/wakun/Downloads/application_1630907351152_49778_dd046396-3f5b-40a4-adcd-c0303781610f.lz4",
      filteredEventTypes = commonFilteredEventTypes ++ Set("SparkListenerTaskEnd"),
      func = {
        case (
            _,
            SparkListenerTaskEnd(
              stageId,
              stageAttemptId,
              _,
              Success,
              taskInfo,
              _,
              taskMetrics)) =>
          val (
            launchTime,
            gettingResultTime,
            finishTime,
            executorDeserializeTime,
            executorRunTime,
            resultSerializationTime,
            jvmGCTime) =
            (
              taskInfo.launchTime,
              taskInfo.gettingResultTime,
              taskInfo.finishTime,
              taskMetrics.executorDeserializeTime,
              taskMetrics.executorRunTime,
              taskMetrics.resultSerializationTime,
              taskMetrics.jvmGCTime)
          val driverTaskTime =
            if (gettingResultTime > launchTime) {
              gettingResultTime - launchTime
            } else {
              finishTime - launchTime
            }
          val scheduleDelayTime = driverTaskTime -
            (executorDeserializeTime + executorRunTime + resultSerializationTime)
          val url =
            s"$viewpointUrl/${appId.get()}/stages/stage/?id=${stageId}&attempt=${stageAttemptId}"
          if (scheduleDelayTime > 4 * 60 * 1000) {
            println(s"ViewPoint URL: ${url}, TaskId: ${taskInfo.taskId}, " +
              s"executorId: ${taskInfo.executorId}, Host: ${taskInfo.host}, " +
              s"scheduleDelayTime: ${scheduleDelayTime}, jvmGCTime =${jvmGCTime}, resultSize = ${taskMetrics.resultSize}")
          }
      })
  }
}
