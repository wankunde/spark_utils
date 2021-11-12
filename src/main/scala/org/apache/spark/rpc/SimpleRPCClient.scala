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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

import org.apache.spark.rpc.messages.SayHi
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf}

object SimpleRPCClient {

  val SERVER_NAME = "OfflineServer"
  val SQL_PARSER_ENDPOINT_NAME = "SQLParser"

  var host = Utils.localHostName()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val securityMgr = new SecurityManager(conf)
    val systemName = SERVER_NAME + "_Client"
    val rpcEnv = RpcEnv.create(systemName, host, -1, conf, securityMgr, clientMode = true)

    // Another way to config RPC server address: masterUrls.map(RpcAddress.fromSparkURL)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(OfflineRPCServer.host, OfflineRPCServer.port), OfflineRPCServer.SQL_PARSER_ENDPOINT_NAME)
    endPointRef.ask[String](SayHi("neo"), new RpcTimeout(30.seconds, "offline.sql.parser"))
      .onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }

    Thread.sleep(1000L)

  }
}
