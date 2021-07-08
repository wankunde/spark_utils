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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.EventLogInputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EVENT_LOG_DIR
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{JsonProtocol, Utils}
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.NonFatal

trait AnalyzeBase extends Logging with Serializable {
  var appId: String = ""
  var executionId: Long = 0
  var appAttemptId: String = ""
  var sqlMapping = mutable.Map[Long, String]()
  val resultDataset = ArrayBuffer[Any]()

  var logdays = 7 // 默认搜索线上7天的任务日志

  val viewpointUrl = "http://viewpoint.hermes-prod.svc.25.tess.io/history"

  val commonFilteredEventTypes = Set(
    "SparkListenerApplicationStart",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate"
  )

  def sparkAnalyze(appName: String,
                   filteredEventTypes: Set[String],
                   func: PartialFunction[(String, SparkListenerEvent), Unit]): Unit = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()
    val sc = spark.sparkContext
    val sparkConf = sc.conf

    val util = SparkHadoopUtil.get
    //!!! Be careful, not HISTORY_LOG_DIR parameter
    val logDir = sparkConf.get(EVENT_LOG_DIR)
    val fs = FileSystem.get(util.conf)

    val now = System.currentTimeMillis()

    Range(0, logdays).foreach(dayBefore => {
      val logFiles = util.listFilesSorted(fs, new Path(logDir), "application_", ".inprogress")
        .filter(fileStatus => {
          val mtime = fileStatus.getModificationTime
          mtime > (now - 86400000 * (dayBefore + 1)) && mtime < now - 86400000 * dayBefore
        })
        .map(_.getPath.toString)

      logInfo(s"Try to analyze ${logFiles.size} log files in ${logDir}")

      sc.hadoopFile[LongWritable, Text, EventLogInputFormat](logFiles.mkString(","))
        .map(_._2.toString)
        .filter(_.checkEventType(filteredEventTypes))
        .foreachPartition { partitionIterator: Iterator[String] =>
          partitionIterator.foreach { json =>
            try {
              val event = JsonProtocol.sparkEventFromJson(parse(json))
              func(json, event)
            } catch {
              case e: Exception =>
                logError(s"Fail to parse log appId = ${appId} log = ${json.substring(0, 100)}", e)
            }
          }
        }
    })

    spark.stop()
  }

  def localAnalyze(filePath: String,
                   filteredEventTypes: Set[String],
                   func: PartialFunction[(String, SparkListenerEvent), Unit]): Unit = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(new Configuration())

    if (!fs.exists(path)) {
      println(s"No such log file: ${filePath}")
      System.exit(-1)
    }
    println(s"========================================CONTENT START FOR ${path}")
    Utils.tryWithResource(EventLogFileReader.openEventLog(path, fs)) { in =>
      val lines =
        Source.fromInputStream(in).getLines()
          .filter(_.checkEventType(filteredEventTypes))
          .toList

      lines.foreach(json => {
        try {
          val event = JsonProtocol.sparkEventFromJson(parse(json))
          func(json, event)
        } catch {
          // ignore any exception occurred from unidentified json
          case NonFatal(_) =>
        }
      })
    }
  }
}
