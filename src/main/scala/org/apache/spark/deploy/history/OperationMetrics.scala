package org.apache.spark.deploy.history

import scala.collection.mutable

case class OperationMetric(accumulatorId: Long, var value: Long, name: String = "")

class OperationMetrics(metrics: mutable.Map[Long, OperationMetric]) {

  val metricNames: mutable.Map[String, Long] =
    metrics.map(kv => kv._2.name -> kv._2.accumulatorId)

  def getOrElseUpdate(metric: OperationMetric): OperationMetric = {
    metrics.getOrElseUpdate(metric.accumulatorId, {
      metricNames.put(metric.name, metric.accumulatorId)
      metric
    })
  }

  def update(accumulatorId: Long, value: Long): Unit =
    getById(accumulatorId).map(metric => metric.value = value)

  def getById(accumulatorId: Long): Option[OperationMetric] =
    metrics.get(accumulatorId)

  def getByName(name: String): Option[OperationMetric] =
    metricNames.get(name)
      .filter(metrics.contains)
      .map(metrics(_))

}

object OperationMetrics {

  def apply(seq: Seq[OperationMetric]): OperationMetrics = {
    val map = mutable.Map[Long, OperationMetric]()
    map ++ seq.map(m => m.accumulatorId -> m.value).toMap

    new OperationMetrics(map)
  }

}
