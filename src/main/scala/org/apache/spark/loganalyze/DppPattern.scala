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
 * 修改 AnalyzeBase 开启Application扫描过滤条件 .filter(p => p.getPath.toString.contains("1636603355091_20415"))
 * rm -rf spark_utils-1.0-bin.tar.gz spark_utils-1.0
 * rz
 * tar -zxvf spark_utils-1.0-bin.tar.gz
 * spark_utils-1.0/bin/spark_utils.sh custom_class org.apache.spark.loganalyze.DppPattern
 * spark_utils-1.0/bin/spark_utils.sh collect_result
 */
object DppPattern extends AnalyzeBase {

  def main(args: Array[String]): Unit = {

    localAnalyze(
//      appName = "Dpp Pattern",
      filePath =
        "/Users/wakun/Downloads/application_1639134104641_3331_43101e28-7da4-4d22-9637-02522f35f6ab.lz4.inprogress",
      filteredEventTypes = commonFilteredEventTypes,
      func = {
        case (_, e: SparkListenerSQLAdaptiveExecutionUpdate, printer) =>
          transformPlanInfo(e.sparkPlanInfo, plan => {
            if (plan.nodeName == "SubqueryBroadcast" && plan.simpleString.startsWith("SubqueryBroadcast dynamicpruning")) {
              printer(s"VIEWPOINT: ${viewPointURL(e.executionId)}")
            }
          })
      })
  }
}
