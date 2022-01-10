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

import org.apache.spark.loganalyze.SearchJoinPattern.{firstExchange, matchJoinPattern}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

object LocalLogReader extends AnalyzeBase {

  def main(args: Array[String]): Unit = {
    localAnalyze(
      filePath = "/Users/wakun/Downloads/application_1624904512119_5728_1_9cd0ba48-910e-41fe-957e-2dc600e68d60.lz4",
      filteredEventTypes = commonFilteredEventTypes,
      func = {
        // if e.sparkPlanInfo.simpleString == "AdaptiveSparkPlan isFinalPlan=true"
        case (json, e: SparkListenerSQLAdaptiveExecutionUpdate, printer) =>
          transformPlanInfo(e.sparkPlanInfo, plan => {
            if (plan.nodeName == "SortMergeJoin") {
              matchJoinPattern(firstExchange(plan.children(0)), firstExchange(plan.children(1))) match {
                case Some((left, right)) =>
                  printer(
                    s"""__BLOCKSTART__URL
                       |${viewPointURL()}
                       |__BLOCKEND__URL
                       |__BLOCKSTART__SQL
                       |${sql()}
                       |__BLOCKEND__SQL""".stripMargin)
                case _ =>
              }
            }
          })
      })
  }
}
