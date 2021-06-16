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

package org.apache.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.history.{EventLogFileReader, SearchJoinPatternUtils}
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.util.{JsonProtocol, Utils}
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source
import scala.util.control.NonFatal

object LocalLogReaderTest {

  def main(args: Array[String]): Unit = {
    val path = new Path(args(0))

    println(s"========================================CONTENT START FOR ${path}")
    val fs = path.getFileSystem(new Configuration())
    Utils.tryWithResource(EventLogFileReader.openEventLog(path, fs)) { in =>
      val lines = Source.fromInputStream(in).getLines().filter { log =>
        log.contains("SparkListenerApplicationStart") ||
          log.contains("org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate")
      }.toList
      var appId: String = ""
      lines.foreach(log => {
        try {
          JsonProtocol.sparkEventFromJson(parse(log)) match {
            case SparkListenerSQLAdaptiveExecutionUpdate(eid, desc, sparkPlanInfo) =>
              if (log.contains("InsertIntoHadoopFsRelationCommand")) {
                val idx = log.indexOf("InsertIntoHadoopFsRelationCommand")
                val insertName: String = log.substring(idx, idx + 300)
                SearchJoinPatternUtils.parsePlanInfo(sparkPlanInfo, appId, insertName)
              }

            case SparkListenerApplicationStart(_, appIdOpt, _, _, _, _, _) if appIdOpt.isDefined =>
              appId = appIdOpt.get

            case _ => // SKIP other events
          }
        } catch {
          // ignore any exception occurred from unidentified json
          case NonFatal(_) =>
        }
      })
    }
  }
}
