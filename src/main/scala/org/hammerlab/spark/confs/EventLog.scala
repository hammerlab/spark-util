package org.hammerlab.spark.confs

import org.hammerlab.spark.SparkConfBase

trait EventLog {
  self: SparkConfBase ⇒

  def listenerBusSize = 1000000

  sparkConf(
    "spark.eventLog.enabled" → "true",
    "spark.yarn.maxAppAttempts" → "1",
    "spark.scheduler.listenerbus.eventqueue.size" → listenerBusSize.toString
  )
}
