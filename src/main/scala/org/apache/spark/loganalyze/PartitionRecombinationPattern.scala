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

import org.apache.spark.loganalyze.AnalyzeBase.sqlProperties
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

/**
 * rm -rf spark_utils*
 * rz
 * tar -zxvf spark_utils-1.0-bin.tar.gz
 * export SPARK_HOME=/mnt/b_carmel/wakun/spark_releases/CARMEL-4978/spark
 * spark_utils-1.0/bin/spark_utils.sh custom_class org.apache.spark.loganalyze.PartitionRecombinationPattern
 * yarn logs -applicationId application_1624904512119_4763 -log_files_pattern stdout | grep -vE 'End of LogType:stdout|^$' | grep -v "\*\*\*"
 */
object PartitionRecombinationPattern extends AnalyzeBase {

  def main(args: Array[String]): Unit = {
    sparkAnalyze(
      appName = "PartitionRecombination Pattern Search",
      filteredEventTypes = commonFilteredEventTypes,
      func = {
        case (_, e: SparkListenerSQLAdaptiveExecutionUpdate, printer) =>
          transformPlanInfo(e.sparkPlanInfo, planInfo => {
            val nodeString = planInfo.simpleString
            if (nodeString.startsWith("SortMergeJoin")) {
              assert(planInfo.children.length >= 2, "Join node should has two children")
              val (left, right) = (planInfo.children(0), planInfo.children(1))
              if ((nodeString.contains("LeftOuter") && left.nodeName == "PartitionRecombination") ||
                (nodeString.contains("RightOuter") && right.nodeName == "PartitionRecombination")) {
                printer(
                  s"""${sql(e.executionId)}
                     |${viewPointURL(e.executionId)}
                     |""".stripMargin)
              }
            }
          })
      })
  }
}
