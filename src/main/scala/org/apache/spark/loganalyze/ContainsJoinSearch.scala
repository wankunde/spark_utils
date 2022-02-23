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

import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

/**
 * <pre>
 * mv ./conf/metrics.properties ./conf/metrics.properties_bak
 *
 * </pre>
 */
object ContainsJoinSearch extends AnalyzeBase {

  def main(args: Array[String]): Unit = {
    sparkAnalyze(
      appName = "Contains Join Search",
      filteredEventTypes = commonFilteredEventTypes,
      func = {
        case (_, e: SparkListenerSQLAdaptiveExecutionUpdate, printer) =>
          transformPlanInfo(e.sparkPlanInfo, plan => {
            if (plan.nodeName == "BroadcastContainsJoin") {
              printer(
                s"""__BLOCKSTART__URL
                   |${viewPointURL(e.executionId)}
                   |__BLOCKEND__URL
                   |__BLOCKSTART__SQL
                   |${sql(e.executionId)}
                   |__BLOCKEND__SQL
                   |""".stripMargin)
            }
          })
      })
  }
}
