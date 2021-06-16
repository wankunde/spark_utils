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

package org.apache.hadoop.mapreduce

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.LineRecordReader
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging

import java.util.{List => JList}
import scala.collection.JavaConverters._

class EventLogInputFormat extends FileInputFormat[LongWritable, Text] with Logging {

  override def isSplitable(context: JobContext, filename: Path): Boolean = false

  override def getSplits(job: JobContext): JList[InputSplit] = {
    val inputPaths = FileInputFormat.getInputPaths(job)
    inputPaths.map { inputPath =>
      // As there are too many log files, skip HDFS RPC call for block locations
      //      val file = fs.getFileStatus(inputPath)
      //      val length = file.getLen
      //      val blkLocations = fs.getFileBlockLocations(file, 0, length)
      //      val splitHosts = getSplitHosts(blkLocations, 0, length, clusterMap)
      makeSplit(inputPath, 0, Integer.MAX_VALUE, null).asInstanceOf[InputSplit]
    }.toList.asJava
  }

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    context.setStatus(split.toString)

    new RecordReader[LongWritable, Text] {

      var key: LongWritable = _

      var value: Text = _

      var lineRecordReader: LineRecordReader = _

      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
        val fileSplit = split.asInstanceOf[FileSplit]
        val fs = FileSystem.get(context.getConfiguration)
        val in = EventLogFileReader.openEventLog(fileSplit.getPath, fs)
        // 原始设计中，如果split是compressed stream, split的read pos 是由codec.createInputStream()
        // 代理流返回的，所以判断没有问题，但是我们这里并不知道当前代理流，pos对应的真实位置。所以，比较Tricky
        // 的办法就是设置read end为Integer.MAX_VALUE， 以读取整个文件
        lineRecordReader = new LineRecordReader(in, 0, fileSplit.getStart, Integer.MAX_VALUE)
      }

      override def nextKeyValue(): Boolean = {
        key = lineRecordReader.createKey()
        value = lineRecordReader.createValue()
        lineRecordReader.next(key, value)
      }

      override def getCurrentKey: LongWritable = key

      override def getCurrentValue: Text = value

      override def getProgress: Float = lineRecordReader.getProgress

      override def close(): Unit = lineRecordReader.close()
    }
  }
}