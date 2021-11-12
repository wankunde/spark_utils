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

import org.apache.spark.sql.catalyst.trees.TreeNode

case class PlanCost(name: String,
                    simpleString: String,
                    rows: Long,
                    children: Seq[PlanCost] = Seq()) extends TreeNode[PlanCost] {

  def nameWithRowCount(): String = {
    val suffix =
      if (priority != 10000) {
        s"priority = $priority"
      } else {
        ""
      }
    s"$name rows = $rows $suffix"
  }

  def priority: Double =
    if (children.size == 0 || children.exists(_.rows == rows)) {
      // 没有子节点进行过滤，或者没有起到过滤效果（存在子节点的记录数 == 结果记录数）
      10000
    } else {
      10000 * rows / children.map(_.rows).max
    }

  override def simpleStringWithNodeId(): String = nameWithRowCount()

  override def verboseString(maxFields: Int): String = nameWithRowCount()
}
