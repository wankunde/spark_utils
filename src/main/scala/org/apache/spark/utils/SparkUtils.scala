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

import org.apache.spark.sql.catalyst.expressions.{DecimalLiteral, DoubleLiteral, FloatLiteral, IntegerLiteral, Literal, StringLiteral}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

/**
 * Using:
 * <pre>
 * mvn install
 * bin/spark-shell --master yarn --num-executors 2 --executor-memory 1G --jars ~/.m2/repository/com/wankun/spark_utils/1.0/spark_utils-1.0.jar
 * import org.apache.spark.sql.SparkUtils._
 * val df  = spark.sql("SELECT * FROM user_sex join user_age ON user_sex.name = user_age.name and user_age.age = 10")
 * df.queryExecution.sparkPlan.withoutLiteral
 * df.queryExecution.sparkPlan.withoutLiteral.semanticHash
 * </pre>
 */
object SparkUtils {
  implicit class SparkPlanUtils(plan: SparkPlan) {
    def withoutLiteral(): SparkPlan = plan transform { case e =>
      e transformExpressions {
        case FloatLiteral(_) => Literal(0.0, FloatType)
        case DoubleLiteral(_) => Literal(0.0, DoubleType)
        case IntegerLiteral(_) => Literal(0, IntegerType)
        case StringLiteral(_) => Literal("", StringType)
        case DecimalLiteral(_) => Literal(Decimal(0))
      }
    }
  }
}
