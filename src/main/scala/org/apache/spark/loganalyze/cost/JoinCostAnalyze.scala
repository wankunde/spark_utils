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

import org.apache.spark.loganalyze._
import org.apache.spark.scheduler.SparkListenerEvent
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

  val sqlDuration = 30

  def main(args: Array[String]): Unit = {
    sparkAnalyze(
      appName = "JoinCostAnalyze",
      filteredEventTypes = commonFilteredEventTypes,
      func
    )
  }

  val func: PartialFunction[(String, SparkListenerEvent, String => Unit), Unit] = {
    case (_, e: SparkListenerSQLAdaptiveExecutionUpdate, _)
      if e.sparkPlanInfo.simpleString == "AdaptiveSparkPlan isFinalPlan=true"
        && sqlProperty(e.executionId).endTime - sqlProperty(e.executionId).startTime > sqlDuration =>
      val planCost = planToPlanCost(e.sparkPlanInfo)
      val simplify = simplifyPlanCost(planCost)
      logInfo(
        s"""planCost:
           |${planCost.treeString}
           |
           |Simplify planCost:
           |${simplify.treeString}
           |""".stripMargin)

      simplify.collect {
        case join if join.name.endsWith("Join") && join.simpleString.contains("Inner") && join.children.size == 2 &&
          (
            (join.children(0).rows > 1000 * 1000 * 100 && isSmallTable(join.children(1))) ||
              (join.children(1).rows > 1000 * 1000 * 100 && isSmallTable(join.children(0)))
            ) &&
          join.rows == join.children.map(_.rows).min
        =>
          println(
            s"""Join Row: ${join.children.map(_.rows)} --> ${join.rows}
               |__BLOCKSTART__URL
               |${viewPointURL(e.executionId)}
               |__BLOCKEND__URL
               |__BLOCKSTART__SQL
               |${sql(e.executionId)}
               |__BLOCKEND__SQL
               |__BLOCKSTART__PLANCOST
               |${planCost.treeString}
               |__BLOCKEND_PLANCOST
               |__BLOCKSTART__SMALLCHILD
               |${join.treeString}
               |__BLOCKEND__SMALLCHILD
               |""".stripMargin)
          join
      }


    /*val joins = planCost.collect { case input if input.name.endsWith("Join") => input }
    var i = 0
    while (joins.size > 5 && i < joins.size - 1) {
      val p1 = joins(i)
      val p2 = joins(i + 1)
      if (p2.rows > 10000L * 10000L * 1000L && p1.rows * 2 < p2.rows) {
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
             |__BLOCKSTART__SMALLCHILD
             |${p1.treeString}
             |__BLOCKEND__SMALLCHILD
             |__BLOCKSTART__LARGECHILD
             |${p2.treeString}
             |__BLOCKEND__LARGECHILD
             |""".stripMargin)
      }
      i = i + 1
    }*/
    /*def sortMergeChildren(input: PlanCost): Option[(PlanCost, PlanCost)] = {
      input collect {
        case parent: PlanCost
          if parent.name == "SortMergeJoin" && parent.children(0).rows > parent.children(1).rows * 1.5 =>
          val Seq(c1, c2) = parent.children
          (c1, c2)

        case parent: PlanCost
          if parent.name == "SortMergeJoin" && parent.children(1).rows > parent.children(0).rows * 1.5 =>
          val Seq(c1, c2) = parent.children
          (c2, c1)
      } headOption
    }

    sortMergeChildren(simplify) map { case (largeChild, smallChild) =>
      sortMergeChildren(largeChild) map { case (largeChild2, smallChild2) =>
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
             |__BLOCKSTART__SMALLCHILD
             |${smallChild.treeString}
             |__BLOCKEND__SMALLCHILD
             |__BLOCKSTART__LARGECHILD
             |${largeChild.treeString}
             |__BLOCKEND__LARGECHILD
             |""".stripMargin)
      }
    }*/
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

  def isSmallTable(planCost: PlanCost): Boolean = {
    planCost.children.size match {
      case 0 =>
        planCost.rows < 100000

      case 1 =>
        if (planCost.rows < 100000 && planCost.children(0).rows > 1000 * 1000) {
          false
        } else {
          isSmallTable(planCost.children(0))
        }

      case _ =>
        false
    }
  }
}
