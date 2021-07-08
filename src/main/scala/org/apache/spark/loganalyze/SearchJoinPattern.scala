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

import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

/**
 * <pre>
 * mv ./conf/metrics.properties ./conf/metrics.properties_bak
 *
 * bin/spark-submit --master yarn --driver-memory 8G \
 * --executor-memory 4G \
 * --class org.apache.spark.deploy.history.SearchJoinPattern \
 * --conf spark.driver.memoryOverhead=0 \
 * --conf spark.driver.extraJavaOptions="" \
 * --conf spark.memory.offHeap.enabled=false \
 * --conf spark.memory.offHeap.size=0 \
 * --conf spark.executor.cores=4 \
 * --conf spark.executor.instances=90 \
 * --conf spark.executor.memory=4g \
 * --conf spark.executor.extraJavaOptions="" \
 * --conf spark.executor.memoryOverhead=0 \
 * --conf spark.yarn.queue=hdmi-staging \
 * ~/wakun/spark_utils-1.0.jar
 *
 * 查看执行结果: yarn logs -applicationId  application_1622159252184_10033 | grep 'Find pattern join'
 * </pre>
 */
object SearchJoinPattern extends AnalyzeBase {

  def main(args: Array[String]): Unit = {
    sparkAnalyze(
      appName = "Search Join Pattern",
      filteredEventTypes = commonFilteredEventTypes,
      func = {
        case (_, SparkListenerSQLAdaptiveExecutionUpdate(_, desc, sparkPlanInfo)) =>
          SearchJoinPatternUtils.parsePlanInfo(sparkPlanInfo, appId)

        case (_, SparkListenerApplicationStart(_, appIdOpt, _, _, _, _, _)) if appIdOpt.isDefined =>
          appId = appIdOpt.get
      })
  }
}
