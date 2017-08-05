package org.hammerlab.spark.confs

import org.hammerlab.spark.SparkConfBase

trait DynamicAllocation {
  self: SparkConfBase ⇒
  sparkConf(
    "spark.shuffle.service.enabled" → "true",
    "spark.dynamicAllocation.enabled" → "true"
  )
}
