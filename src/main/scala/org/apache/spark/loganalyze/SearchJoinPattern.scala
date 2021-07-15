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
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

/**
 * <pre>
 * mv ./conf/metrics.properties ./conf/metrics.properties_bak
 *
 * </pre>
 */
object SearchJoinPattern extends AnalyzeBase {

  def main(args: Array[String]): Unit = {
    sparkAnalyze(
      appName = "Search Join Pattern",
      filteredEventTypes = commonFilteredEventTypes,
      func = {
        case (_, e: SparkListenerSQLAdaptiveExecutionUpdate) =>
          sqlProperties.get.executionId = e.executionId
          transformPlanInfo(e.sparkPlanInfo, plan => {
            if (plan.nodeName == "SortMergeJoin") {
              matchJoinPattern(firstExchange(plan.children(0)), firstExchange(plan.children(1))) match {
                case Some((left, right)) =>
                  println(
                    s"""__BLOCKSTART__URL
                       |${sqlProperties.get.viewPointURL}
                       |__BLOCKEND__URL
                       |__BLOCKSTART__SQL
                       |${sqlProperties.get.sql}
                       |__BLOCKEND__SQL
                       |""".stripMargin)
                case _ =>
              }
            }
          })
      })
  }

  /**
   * must return a node
   *
   * @param node
   * @return
   */
  def firstExchange(node: SparkPlanInfo): Option[SparkPlanInfo] = {
    if (node.nodeName == "Exchange") {
      Some(node)
    } else if (node.nodeName == "CustomShuffleReader") {
      val coalesceNumberOpt =
        node.metrics.filter(_.name == "number of partitions")
          .collectFirst { case m => sqlProperties.get.getMetricById(m.accumulatorId) }
      val partitionNumOpt = hashPartitionNumberOpt(node)
      if(coalesceNumberOpt.isDefined && coalesceNumberOpt.get == -1) {
        println(s"Becareful with ${node.simpleString} in ${sqlProperties.get.viewPointURL}, no coalesce number!")
      }
      if(partitionNumOpt.isDefined && partitionNumOpt.get ==0) {
        println(s"Becareful with ${node.simpleString} in ${sqlProperties.get.viewPointURL}, no hash partition number")
      }
      if (partitionNumOpt.isDefined && coalesceNumberOpt.isDefined &&
        partitionNumOpt.get != coalesceNumberOpt.get) {
        Some(node)
      } else {
        None
      }
    } else if (node.nodeName == "BroadcastExchange" ||
      node.nodeName.contains("Scan parquet") ||
      node.nodeName.contains("AdaptiveSparkPlan")) {
      None
    } else {
      node.children.map(firstExchange(_)).find(_.isDefined).getOrElse(None)
    }
  }

  def matchJoinPattern(node1Opt: Option[SparkPlanInfo],
                       node2Opt: Option[SparkPlanInfo]): Option[(SparkPlanInfo, SparkPlanInfo)] = {
    if (node1Opt == None || node2Opt == None) {
      None
    } else {
      val node1 = node1Opt.get
      val node2 = node2Opt.get
      if (node1.nodeName == "Exchange" && node2.simpleString == "CustomShuffleReader coalesced") {
        Some((node1, node2))
      } else if (node2.nodeName == "Exchange" && node1.simpleString == "CustomShuffleReader coalesced") {
        Some((node2, node1))
      } else {
        None
      }
    }
  }
}
