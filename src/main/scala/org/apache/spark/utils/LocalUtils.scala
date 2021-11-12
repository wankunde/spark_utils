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

package org.apache.spark.utils

import java.io.{BufferedReader, File, InputStreamReader}

import scala.collection.mutable.ArrayBuffer

object Utils {

  def runSystemCommand(cmds: Array[String], workingDir: File): ArrayBuffer[String] = {
    var process: java.lang.Process = null
    var bufrIn: BufferedReader = null
    var bufrError: BufferedReader = null

    try {
      process = Runtime.getRuntime().exec(cmds, null, workingDir);
      // 方法阻塞, 等待命令执行完成（成功会返回0）
      process.waitFor();
      bufrIn = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
      bufrError = new BufferedReader(new InputStreamReader(process.getErrorStream(), "UTF-8"));

      val result = ArrayBuffer[String]()
      var line: String = null
      do {
        line = bufrIn.readLine()
        if (line != null) {
          result += line
        }
      } while (line != null)
      do {
        line = bufrError.readLine()
        if (line != null) {
          result += line
        }
      } while (line != null)
      result

    } finally {
      if (bufrIn != null) {
        try {
          bufrIn.close()
        }
      }
      if (bufrError != null) {
        try {
          bufrError.close()
        }
      }
      // 销毁子进程
      if (process != null) {
        process.destroy();
      }
    }
  }

}
