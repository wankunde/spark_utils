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

import org.apache.spark.sql.execution.SparkPlanInfo

object SearchJoinPatternUtils {

  def parsePlanInfo(plan: SparkPlanInfo, appId: String): Unit = {
    val nodeName = plan.nodeName
    if (nodeName.contains("InsertIntoHadoopFsRelationCommand") ||
      nodeName.contains("OptimizedCreateHiveTableAsSelectCommand")) {
      val simpleString = plan.simpleString
    }
    if (nodeName == "SortMergeJoin") {
      matchJoinPattern(firstExchange(plan.children(0)), firstExchange(plan.children(1))) match {
        case Some((left, right)) =>
          println(s"Find pattern join in appId: $appId")
        case _ =>
      }
    }
    plan.children.map(parsePlanInfo(_, appId))
  }

  /**
   * must return a node
   *
   * @param node
   * @return
   */
  def firstExchange(node: SparkPlanInfo): Option[SparkPlanInfo] = {
    if (node.nodeName == "Exchange" || node.nodeName == "CustomShuffleReader") {
      Some(node)
    } else if (node.nodeName.contains("Join") ||
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
