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

package org.apache.spark.rpc

import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf}

object OfflineServer {

  val SERVER_NAME = "OfflineServer"
  val SQL_PARSER_ENDPOINT_NAME = "SQLParser"

  var host = Utils.localHostName()
  var port = 5188

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SERVER_NAME, host, port, conf, securityMgr)
    val sqlParserEndpoint = rpcEnv.setupEndpoint(SQL_PARSER_ENDPOINT_NAME, new SqlParserEndPoint(rpcEnv))
    rpcEnv.awaitTermination()
  }

}
