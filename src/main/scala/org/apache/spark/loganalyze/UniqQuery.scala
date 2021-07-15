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

import scala.collection.mutable
import scala.io.Source

object UniqQuery {

  def main(args: Array[String]): Unit = {
    if (new File("/tmp/AnalyzeBase.log").exists()) {
      println("Temp log file /tmp/AnalyzeBase.log not exists, please check your program!")
      System.exit(-1)
    }

    val file = Source.fromFile("/tmp/AnalyzeBase.log")
    var inBlock = false
    var readURL = false
    var sql = ""
    var url = ""
    val sqlAndURL = mutable.Map[String, String]()
    var buf = new StringBuffer()
    for (line <- file.getLines) {
      line match {
        case "__BLOCKSTART__SQL" | "__BLOCKSTART__URL" =>
          inBlock = true

        case "__BLOCKEND__SQL" =>
          buf.deleteCharAt(buf.length() - 1)
          sql = buf.toString
          buf = new StringBuffer()

          sqlAndURL.put(sql, url)
          inBlock = false

        case "__BLOCKEND__URL" =>
          buf.deleteCharAt(buf.length() - 1)
          url = buf.toString
          buf = new StringBuffer()
          inBlock = false

        case line =>
          if (inBlock) {
            buf.append(line + "\n")
          }
      }
    }
    file.close

    // print result
    for ((sql, url) <- sqlAndURL) {
      println(
        s"""__BLOCKSTART__URL
           |$url
           |__BLOCKEND__URL
           |__BLOCKSTART__SQL
           |$sql
           |__BLOCKEND__SQL
           |""".stripMargin)
    }
  }

}
