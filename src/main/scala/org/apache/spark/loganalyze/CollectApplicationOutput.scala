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

import java.io.File

import scala.io.Source.fromFile
import scala.reflect.io.{Directory, Path}

import org.apache.spark.utils.LocalUtils._

/**
 * spark_utils.sh collect_result
 */
object CollectApplicationOutput {

  def main(args: Array[String]): Unit = {
    val outputPath = "/tmp/spark_utils_stdout"
    val appId = fromFile(SPARK_APPLICATION_ID_PATH).mkString
    println(s"appId: ${appId}")
    val outputDir = Directory(Path(outputPath))
    outputDir.delete()
    val cmd =
      s"yarn logs -applicationId ${appId} -log_files_pattern stdout -out ${outputPath}"
    println("Getting application output")
    runSystemCommand {
      Runtime.getRuntime().exec(cmd, null, new File("/tmp"))
    }

    val outputs =
      outputDir.deepFiles
        .map(_.jfile)
        .flatMap {
          fromFile(_)
            .getLines()
            .filter(_.trim.length > 0)
            .filterNot(line =>
              line.startsWith("************************") ||
                line.startsWith("End of LogType:stdout") ||
                line.startsWith("LogAggregationType: ") ||
                line.startsWith("LogType:stdout") ||
                line.startsWith("LogLastModifiedTime") ||
                line.startsWith("LogLength:") ||
                line.startsWith("LogContents:")
            )
        }.mkString("\n")
    println(outputs)
    println("End")
  }
}
