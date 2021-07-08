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
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionStart}

object LocalLogReader extends AnalyzeBase {

  def main(args: Array[String]): Unit = {
    localAnalyze(
      filePath = args(0),
      filteredEventTypes = commonFilteredEventTypes,
      func = {
        case (_, e: SparkListenerApplicationStart) if e.appId.isDefined =>
          appId = e.appId.get
          appAttemptId = e.appAttemptId.getOrElse("")

        case (_, e: SparkListenerSQLExecutionStart) =>
          sqlMapping(e.executionId) = e.description

        case (_, e: SparkListenerSQLAdaptiveExecutionUpdate) =>
          executionId = e.executionId
          findJoinWithPartitionRecombination(e.sparkPlanInfo)
      }
    )

  }

  def findJoinWithPartitionRecombination(planInfo: SparkPlanInfo): Unit = {
    val nodeString = planInfo.simpleString
    if (nodeString.startsWith("SortMergeJoin")) {
      assert(planInfo.children.length == 2, "Join node should has two children")
      val (left, right) = (planInfo.children(0), planInfo.children(1))
      if ((nodeString.contains("LeftOuter") && left.nodeName == "PartitionRecombination") ||
        (nodeString.contains("RightOuter") && right.nodeName == "PartitionRecombination")) {
        resultDataset += (sqlMapping.getOrElse(executionId, ""))
        println(sqlMapping.getOrElse(executionId, ""))
        println(s"$viewpointUrl/$appId/$appAttemptId/SQL/execution/?id=$executionId")
      }
    }
    planInfo.children.foreach(findJoinWithPartitionRecombination)
  }
}
