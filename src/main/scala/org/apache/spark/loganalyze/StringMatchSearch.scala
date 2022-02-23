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
 * cd /Users/wakun/ws/wankun/spark_utils
 * mvn clean package -DskipTests=true
 * scp -P 1234 /Users/wakun/ws/wankun/spark_utils/target/spark_utils-1.0-bin.tar.gz localhost:/home/b_carmel/wakun/files
 *
 * cd /home/b_carmel/wakun/spark_utils
 * rm -rf spark_utils-1.0  spark_utils-1.0-bin.tar.gz
 * cp /home/b_carmel/wakun/files/spark_utils-1.0-bin.tar.gz .
 * tar -zxvf spark_utils-1.0-bin.tar.gz
 * spark_utils-1.0/bin/spark_utils.sh custom_class org.apache.spark.loganalyze.StringMatchSearch
 * spark_utils-1.0/bin/spark_utils.sh collect_result
 */
object StringMatchSearch extends AnalyzeBase {

  def main(args: Array[String]): Unit = {

    sparkAnalyze(
      appName = "StringMatchSearch",
//      filePath =
//        "/Users/wakun/Downloads/application_1639536657631_17257_7e67fe9f-8816-4355-b58a-e66dd740ff0f.lz4",
//      "/Users/wakun/Downloads/application_1639536657631_16401_fbfbe740-b3e6-4823-b649-b27f8b17ea6f.lz4",
      filteredEventTypes = commonFilteredEventTypes,
      func = {
        case (_, e: SparkListenerSQLAdaptiveExecutionUpdate, printer) =>
          transformPlanInfo(e.sparkPlanInfo, plan => {
            matchPattern(plan.nodeName, plan.simpleString) match {
              case Some(("NestedLoopJoin", matchFunc)) =>
                sqlProperty(e.executionId).endTime - sqlProperty(e.executionId).startTime
                val meta = Seq(
                  sqlProperty(e.executionId).queryId,
                  sqlProperty(e.executionId).startTime,
                  sqlProperty(e.executionId).endTime,
                  (sqlProperty(e.executionId).endTime - sqlProperty(e.executionId).startTime) / 1000,
                  matchFunc
                ).mkString("\t")
                printer(s"""=== Link: ${viewPointURL(e.executionId)}
                           |===META:  ${meta}
                           |${sql(e.executionId)}
                           |
                           |
                           |""".stripMargin)
              case _ =>
            }
          })
      })
  }

  def matchPattern(nodeName: String, simpleString: String): Option[(String, String)] = {
    if (nodeName == "BroadcastNestedLoopJoin" && simpleString.contains(" LIKE ")) {
      Some(("NestedLoopJoin", "LIKE"))
    } else if(nodeName == "BroadcastNestedLoopJoin" && simpleString.contains("position(")) {
      Some(("NestedLoopJoin", "POSITION"))
    } else {
      None
    }
  }
}
