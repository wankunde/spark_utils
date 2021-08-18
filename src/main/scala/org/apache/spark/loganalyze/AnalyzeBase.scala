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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.EventLogInputFormat
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EVENT_LOG_DIR
import org.apache.spark.loganalyze.AnalyzeBase._
import org.apache.spark.scheduler.{AccumulableInfo, SparkListenerApplicationStart, SparkListenerEvent, SparkListenerStageCompleted}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.util.{JsonProtocol, Utils}

trait AnalyzeBase extends Logging with Serializable {
  var logdays = 7 // 默认搜索线上7天的任务日志

  val viewpointUrl = "http://viewpoint.hermes-prod.svc.25.tess.io/history"

  val commonFilteredEventTypes = Set(
    // events for metrics
    "SparkListenerStageCompleted",
    "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",

    //appId and appAttemptId
    "SparkListenerApplicationStart",

    // sql description and duration
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",

    // sql physical plan
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
          try {
            val jsonAndEvent = new ArrayBuffer[(String, SparkListenerEvent)]()
            partitionIterator.foreach { json =>
              try {
                val event: SparkListenerEvent = JsonProtocol.sparkEventFromJson(parse(json))
                event match {
                  case e: SparkListenerDriverAccumUpdates =>
                    accumUpdates.get() ++= e.accumUpdates.toMap

                  case e: SparkListenerStageCompleted =>
                    accumUpdates.get() ++=
                      e.stageInfo.accumulables.values.map {
                        case acc: AccumulableInfo if acc.value.isDefined =>
                          val value: Long = acc.value.get match {
                            case l: Long => l
                            case s: String =>
                              // skip values which can not be parsed to long,
                              // like "List([name: dw_checkout_trans_gdpr, prunedFiles: 9618, prunedRowGroups: 1282])"
                              try {
                                s.toLong
                              } catch {
                                case _ => 0
                              }
                          }
                          acc.id -> value
                      }.toMap

                  case e: SparkListenerApplicationStart =>
                    appId.set(e.appId.get)
                    appAttemptId.set(e.appAttemptId.getOrElse(""))

                  case e: SparkListenerSQLExecutionStart =>
                    val sqlProperty = SQLProperties()
                    sqlProperty.startTime = e.time
                    sqlProperty.sql = e.description
                    sqlProperties.get += e.executionId -> sqlProperty

                  case e: SparkListenerSQLExecutionEnd =>
                    sqlProperty(e.executionId).endTime = e.time

                  case _ =>
                }
                jsonAndEvent += Tuple2(json, event: SparkListenerEvent)
              } catch {
                case _ =>
              }
            }

            catchAnalyzeException {
              jsonAndEvent.collect {
                case (_, e: SparkListenerSQLExecutionStart) =>
                  executionIdOpt.set(e.executionId)

                case (_, e: SparkListenerSQLExecutionEnd) =>
                  executionIdOpt.set(0L)

                case (json, event) if func.isDefinedAt(json, event) => func(json, event)
              }
            }
          } catch {
            case e: Exception =>
              logError(s"Failed to analyze spark log. $e")
          }

          sqlProperties.remove()
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
      val jsonAndEvent =
        lines.flatMap(json => try {
          val event = JsonProtocol.sparkEventFromJson(parse(json))
          event match {
            case e: SparkListenerDriverAccumUpdates =>
              accumUpdates.get() ++= e.accumUpdates.toMap

            case e: SparkListenerStageCompleted =>
              accumUpdates.get() ++=
                e.stageInfo.accumulables.values.map {
                  case acc: AccumulableInfo if acc.value.isDefined =>
                    val value: Long = acc.value.get match {
                      case l: Long => l
                      case s: String =>
                        // skip values which can not be parsed to long,
                        // like "List([name: dw_checkout_trans_gdpr, prunedFiles: 9618, prunedRowGroups: 1282])"
                        try {
                          s.toLong
                        } catch {
                          case _ => 0
                        }
                    }
                    acc.id -> value
                }.toMap

            case e: SparkListenerApplicationStart =>
              appId.set(e.appId.get)
              appAttemptId.set(e.appAttemptId.getOrElse(""))

            case e: SparkListenerSQLExecutionStart =>
              val sqlProperty = SQLProperties()
              sqlProperty.startTime = e.time
              sqlProperty.sql = e.description
              sqlProperties.get += e.executionId -> sqlProperty

            case e: SparkListenerSQLExecutionEnd =>
              sqlProperty(e.executionId).endTime = e.time

            case _ =>
          }
          Seq((json, event))

        } catch {
          case _ => Seq()
        })

      catchAnalyzeException {
        jsonAndEvent.collect {
          case (_, e: SparkListenerSQLExecutionStart) =>
            executionIdOpt.set(e.executionId)

          case (_, e: SparkListenerSQLExecutionEnd) =>
            executionIdOpt.set(0L)

          case (json, event) if func.isDefinedAt(json, event) => func(json, event)
        }
      }
      sqlProperties.remove()
    }
  }

  def catchAnalyzeException(body: => Any): Any = {
    try {
      body
    } catch {
      case e: AnalyzeException =>
        throw e

      // ignore any exception occurred from unidentified json
      case NonFatal(p) =>
        throw p
    }
  }

  def sqlProperty(executionId: Long = executionIdOpt.get()): SQLProperties =
    sqlProperties.get.getOrElse(executionId, SQLProperties())

  def sql(executionId: Long = executionIdOpt.get()): String =
    sqlProperty(executionId).sql

  def viewPointURL(executionId: Long = executionIdOpt.get()): String =
    s"$viewpointUrl/${appId.get()}/${appAttemptId.get()}/SQL/execution/?id=$executionId"

  def metric(accumulatorId: Long): Long =
    accumUpdates.get.getOrElse(accumulatorId, -1)
}

object AnalyzeBase {
  val sqlProperties = new ThreadLocal[mutable.Map[Long, SQLProperties]]() {
    override def initialValue() = new mutable.HashMap[Long, SQLProperties]()
  }

  val appId = new ThreadLocal[String]() {
    override def initialValue() = ""
  }

  val appAttemptId = new ThreadLocal[String]() {
    override def initialValue() = ""
  }

  val executionIdOpt = new ThreadLocal[Long]() {
    override def initialValue(): Long = 0L
  }

  val accumUpdates = new ThreadLocal[mutable.Map[Long, Long]]() {
    override def initialValue() = new mutable.HashMap[Long, Long]()
  }
}

case class SQLProperties(var executionId: Long = 0,
                         var sql: String = "",
                         var startTime: Long = 0,
                         var endTime: Long = 0)