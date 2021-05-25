package org.apache.hadoop.mapred

import com.google.common.base.Charsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeys, FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.net.NetworkTopology
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging

import java.io.InputStream
import java.nio.ByteBuffer

class EventLogInputFormat extends FileInputFormat[LongWritable, Text]
  with JobConfigurable
  with Logging {

  override def isSplitable(fs: FileSystem, file: Path): Boolean = false

  override def getSplits(job: JobConf, numSplits: Int) = {
    val fs = FileSystem.get(job)
//    val clusterMap = new NetworkTopology()
    val inputPaths = FileInputFormat.getInputPaths(job)
    inputPaths.map { inputPath =>
      // As there are too many log files, skip HDFS RPC call for block locations
//      val file = fs.getFileStatus(inputPath)
//      val length = file.getLen
//      val blkLocations = fs.getFileBlockLocations(file, 0, length)
//      val splitHosts = getSplitHosts(blkLocations, 0, length, clusterMap)
      makeSplit(inputPath, 0, Integer.MAX_VALUE, null)
    }
  }

  override def getRecordReader(split: InputSplit,
                               job: JobConf,
                               reporter: Reporter): RecordReader[LongWritable, Text] = {
    val fileSplit = split.asInstanceOf[FileSplit]
    val fs = FileSystem.get(job)
    reporter.setStatus(split.toString)
    val in = EventLogFileReader.openEventLog(fileSplit.getPath, fs)
    // 原始设计中，如果split是compressed stream, split的read pos 是由codec.createInputStream()
    // 代理流返回的，所以判断没有问题，但是我们这里并不知道当前代理流，pos对应的真实位置。所以，比较Tricky
    // 的办法就是设置read end为Integer.MAX_VALUE， 以读取整个文件
    new LineRecordReader(in, fileSplit.getStart, Integer.MAX_VALUE, job)
  }

  override def configure(job: JobConf): Unit = {}
}
