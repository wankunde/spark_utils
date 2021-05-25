package org.apache.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.util.Utils

import scala.io.Source

/**
 * Using:
 * <pre>
 * mvn install
 * bin/spark-submit --master local --class org.apache.spark.core.EventLogReader ~/.m2/repository/com/wankun/spark_utils/1.0/spark_utils-1.0.jar  [fileName]
 * </pre>
 */
object EventLogReader {

  def main(args: Array[String]): Unit = {
    val path = new Path(args(0))

    val fs = path.getFileSystem(new Configuration())
    println(s"========================================CONTENT START FOR ${path}")
    Utils.tryWithResource(EventLogFileReader.openEventLog(path, fs)) { in =>
      val lines = Source.fromInputStream(in).getLines().toList
      if (args.length == 1) {
        lines.foreach(println)
      } else if (args.length == 2) {
        val filterContent = args(1)
        lines.filter(_.contains(filterContent)).foreach(println)
      }
    }
    println(s"========================================CONTENT END FOR ${path}")
  }
}
