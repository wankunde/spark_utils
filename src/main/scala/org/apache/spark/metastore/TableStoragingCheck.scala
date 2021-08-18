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

package org.apache.spark.metastore

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object TableStorageDiretoryCheck extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Table storage directory Check")
      .getOrCreate()

    val sc = spark.sparkContext
    val catalog = spark.sessionState.catalog
    val databases = catalog.listDatabases()
    println(s"Database number: ${databases.size}")
    databases.foreach { database =>
      val tables = catalog.listTables(database)
      tables.foreach { table =>
        val catalogTable = catalog.getTableMetadata(table)

        catalogTable.storage
      }

      tables

    }

  }
}
