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

package org.apache.spark.loganalyze.cost

import org.apache.spark.loganalyze.AnalyzeBase
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

/**
 * rm -rf spark_utils*
 * rz
 * tar -zxvf spark_utils-1.0-bin.tar.gz
 * export SPARK_HOME=/mnt/b_carmel/wakun/spark_releases/CARMEL-4978/spark
 * spark_utils-1.0/bin/spark_utils.sh custom_class org.apache.spark.loganalyze.PartitionRecombinationPattern
 * yarn logs -applicationId application_1624904512119_4763 -log_files_pattern stdout | grep -vE 'End of LogType:stdout|^$' | grep -v "\*\*\*"
 */
object JoinCostAnalyze extends AnalyzeBase {

  def main(args: Array[String]): Unit = {
    sparkAnalyze(
      //      filePath = "/Users/wakun/Downloads/application_1627443888187_3898_1_b0f538a0-3038-4fa6-aa62-d97e8f57b5fb.lz4",
      appName = "JoinCostAnalyze",
      filteredEventTypes = commonFilteredEventTypes,
      func = {
        case (_, e: SparkListenerSQLAdaptiveExecutionUpdate)
          if e.sparkPlanInfo.simpleString == "AdaptiveSparkPlan isFinalPlan=true"
            && sqlProperty(e.executionId).endTime - sqlProperty(e.executionId).startTime > 30 * 60 * 60 =>
          val planCost = planToPlanCost(e.sparkPlanInfo)
          val simplify = simplifyPlanCost(planCost)
          logDebug(
            s"""planCost:
               |${planCost.treeString}
               |
               |Simplify planCost:
               |${simplify.treeString}
               |""".stripMargin)

          simplify collect { case parent if parent.name == "SortMergeJoin" =>
            val childPlanOpt = parent.collectFirst {
              case child
                if child != parent && child.name == "SortMergeJoin"
                  && parent.priority < child.priority && child.priority < 10000 =>
                child
            }
            if (childPlanOpt.isDefined) {
              println(
                s"""__BLOCKSTART__URL
                   |${viewPointURL(e.executionId)}
                   |__BLOCKEND__URL
                   |__BLOCKSTART__SQL
                   |${sql(e.executionId)}
                   |__BLOCKEND__SQL
                   |__BLOCKSTART__PLANCOST
                   |${planCost.treeString}
                   |__BLOCKEND_PLANCOST
                   |__BLOCKSTART__PARENT
                   |${parent.treeString}
                   |__BLOCKEND__PARENT
                   |""".stripMargin)
            }
          }
      })
  }

  def extractTableScan(name: String): TableIdentifier = {
    val tableNameWithDBAndColumns = name.split(" ")(2)
    val idx = tableNameWithDBAndColumns.indexOf('[')
    val tableNameWithDB = tableNameWithDBAndColumns.substring(0, idx)
    val Array(dbName, tableName) = tableNameWithDB.split('.')
    new TableIdentifier(tableName, Some(dbName))
  }

  def extractJoin(simpleString: String): (String, Seq[String], Seq[String], String) = try {
    var lastIdx = 0
    var idx = simpleString.indexOf('[', lastIdx + 1)
    if (idx < 0) {
      idx = simpleString.length
    }
    val nodeName =
      simpleString.substring(lastIdx, idx).trim

    lastIdx = idx + 1
    idx = simpleString.indexOf(']', lastIdx + 1)
    if (idx < 0) {
      idx = simpleString.length
    }
    val leftCondition = simpleString.substring(lastIdx, idx).split(',').map(_.split('#')(0).trim)

    lastIdx = simpleString.indexOf('[', idx + 1) + 1
    idx = simpleString.indexOf(']', lastIdx + 1)
    if (idx < 0) {
      idx = simpleString.length
    }
    val rightCondition = simpleString.substring(lastIdx, idx).split(',').map(_.split('#')(0).trim)

    lastIdx = simpleString.indexOf(',', idx + 1) + 1
    idx = simpleString.indexOf(',', lastIdx + 1)
    if (idx < 0) {
      idx = simpleString.length
    }
    val joinType = simpleString.substring(lastIdx, idx).trim
    (nodeName, leftCondition, rightCondition, joinType)
  } catch {
    case e =>
      logInfo(s"Failed to parse ${simpleString}")
      ("", Seq(), Seq(), "")
  }

  def planToPlanCost(plan: SparkPlanInfo): PlanCost = {
    val children = plan.children.map(planToPlanCost)
    getPlanCost(plan) match {
      case Some(cost) => PlanCost(plan.nodeName, plan.simpleString, cost, children)
      case None =>
        if (children.size == 1) {
          children(0)
        } else {
          PlanCost(plan.nodeName, plan.simpleString, 0, children)
        }
    }
  }

  def getPlanCost(plan: SparkPlanInfo): Option[Long] = {
    // assume will never return -1
    plan.metrics.filter(_.name == "number of output rows")
      .collectFirst { case m => metric(m.accumulatorId) }
  }

  def simplifyPlanCost(planCost: PlanCost): PlanCost = {
    val children = planCost.children.map(simplifyPlanCost)
    if (children.size == 1 && planCost.name != "HashAggregate") {
      children(0).copy(rows = planCost.rows)
      //.copy(rows = planCost.rows)
    } else {
      planCost.copy(children = children)
    }
  }
}
