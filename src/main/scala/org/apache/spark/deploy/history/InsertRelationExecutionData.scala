package org.apache.spark.deploy.history

case class InsertRelationExecutionData(executionId: Long,
                                       description: String,
                                       simpleString: String,
                                       metrics: OperationMetrics)
