package org.hammerlab.spark.confs

import org.hammerlab.spark.SparkConfBase

trait Speculation {
  self: SparkConfBase ⇒
  def speculationInterval = 1000
  def speculationMultiplier = 1.3
  sparkConf(
    "spark.speculation" → "true",
    "spark.speculation.interval" → speculationInterval.toString,
    "spark.speculation.multiplier" → speculationMultiplier.toString
  )
}
