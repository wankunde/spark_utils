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

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.commons.mail.SimpleEmail
import org.apache.log4j.Logger

/**
 * All methods can run without spark package
 */
object LocalUtils {
  private val logger = Logger.getLogger(this.getClass)

  val SPARK_APPLICATION_ID_PATH = "/tmp/_application_id"

  def runSystemCommand(createProcess: => Process): Unit = {
    var process: java.lang.Process = null

    try {
      process = createProcess
      // 方法阻塞, 等待命令执行完成（成功会返回0）
      process.waitFor();
    } finally {
      // 销毁子进程
      if (process != null) {
        process.destroy();
      }
    }
  }

  def sendEmail(
      smtpHost: String,
      from: String,
      to: String,
      subject: String,
      message: String): Unit = {
    val email = new SimpleEmail()
    email.setHostName(smtpHost);
    email.addTo(to)
    email.setFrom(from)
    email.setCharset("UTF-8");
    email.setSubject(subject)
    email.setMsg(message)
    email.send()
  }

  def createConnection(jdbcId: String): Connection = {
    val classLoader = Thread.currentThread().getContextClassLoader
    val prop = new Properties()
    prop.load(classLoader.getResourceAsStream("./sink.jdbc.properties"))
    if (prop.containsKey(s"$jdbcId.driver")) {
      val driver = prop.getProperty(s"$jdbcId.driver")
      Class.forName(driver)
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
    } catch {
      case e: Exception =>
        logger.error("Execute JDBC error!", e)
        throw e
    } finally {
      conn.rollback()
      conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    withConnection("mysql") { conn =>
      val pstmt = conn.prepareStatement("INSERT INTO test1 VALUES(?, ?)")
      val ds = Range(5, 10).map(i => (i, "wankun_" + i))
      ds.map {
        case (id, name) =>
          pstmt.setInt(1, id)
          pstmt.setString(2, name)
          println(s"$id --> $name")

          pstmt.addBatch()
      }
      pstmt.executeBatch()
    }
  }
}
