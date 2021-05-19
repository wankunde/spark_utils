package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._
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
