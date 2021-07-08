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

import org.apache.spark.utils.JDBCUtils.withConnection
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class JDBCUtilsSuite extends AnyFunSuite with BeforeAndAfter {

  test("sink data to jdbc table") {
    withConnection("h2") { conn =>
      conn.prepareStatement(
        "create table people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
      conn.prepareStatement("insert into people values ('fred', 1)").executeUpdate()
      conn.prepareStatement("insert into people values ('mary', 2)").executeUpdate()
      val rs = conn.prepareStatement("select count(1) from people").executeQuery()
      if (rs.next()) {
        assert(rs.getInt(1) === 2)
      }
    }
  }

}
