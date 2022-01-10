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

/**
 *
 */
object CatLog extends AnalyzeBase {
  def main(args: Array[String]): Unit = {
    localAnalyze(
      filePath = "/Users/wakun/Downloads/application_1639536657631_17257_7e67fe9f-8816-4355-b58a-e66dd740ff0f.lz4",
      filteredEventTypes = Set.empty[String],
      func = {
        case (json, _, printer) =>
          if(json.contains("95768577-69ca-4eea-be26-da654b0abce6"))
            printer(json)
      }
    )
  }
}
