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
 * java -cp spark_utils-1.0/libs/spark_utils-1.0.jar org.apache.spark.loganalyze.CollectApplicationOutput mx.vip.ebay.com DL-eBay-CCOE-BTD@ebay.com wakun@ebay.com
 */
object CollectApplicationOutput {

  def main(args: Array[String]): Unit = {
//    val smtpHost = args(0)
//    val from = args(1)
//    val to = args(2)

    val outputPath = "/tmp/spark_utils_stdout"
    val appId = fromFile(SPARK_APPLICATION_ID_PATH).mkString
    val outputDir = Directory(Path(outputPath))
    outputDir.delete()
    val cmd =
      s"yarn logs -applicationId ${appId} -log_files_pattern stdout -out ${outputPath}"
    println("Getting application output")
    runSystemCommand {
      Runtime.getRuntime().exec(cmd, null, new File("/tmp"))
    }
    println("Sending email")

    val outputs =
      outputDir.deepFiles
        .map(_.jfile)
        .flatMap {
          fromFile(_)
            .getLines()
            .filter(_.trim.length > 0)
            .filterNot(_.contains("************************"))
            .filterNot(_.contains("End of LogType:stdout"))
        }.mkString("\n")
//    sendEmail(smtpHost, from, to, "Schedule Delay Detector", outputs)
    println(outputs)
    println("End")
  }
}
