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

import org.apache.spark.util.Utils

import java.sql.{Connection, DriverManager}
import java.util.Properties

object JDBCUtils {

  def createConnection(jdbcId: String): Connection = {
    val classLoader = Thread.currentThread().getContextClassLoader
    val prop = new Properties()
    prop.load(classLoader.getResourceAsStream("./sink.jdbc.properties"))
    if (prop.containsKey(s"$jdbcId.driver")) {
      val driver = prop.getProperty(s"$jdbcId.driver")
      Utils.classForName(driver)
    }
    val url = prop.getProperty(s"$jdbcId.url")
    val username = prop.getProperty(s"$jdbcId.username")
    val password = prop.getProperty(s"$jdbcId.password")

    DriverManager.getConnection(url, username, password)
  }

  def withConnection[T](jdbcId: String)(body: Connection => T): T = {
    val conn = createConnection(jdbcId)
    try {
      conn.setAutoCommit(false)
      val res = body(conn)
      conn.commit()
      res
    } finally {
      conn.rollback()
      conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    withConnection("mysql") { conn =>
      val pstmt = conn.prepareStatement(
        "INSERT INTO test1 VALUES(?, ?)"
      )
      val ds = Range(5, 10).map(i => (i, "wankun_" + i))
      ds.map { case (id, name) =>
        pstmt.setInt(1, id)
        pstmt.setString(2, name)
        println(s"$id --> $name")

        pstmt.addBatch()
      }
      pstmt.executeBatch()
    }
  }
}
