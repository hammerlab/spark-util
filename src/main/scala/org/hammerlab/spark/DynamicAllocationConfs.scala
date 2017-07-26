package org.hammerlab.spark

trait DynamicAllocationConfs {
  self: SparkConfBase ⇒
  sparkConf(
    "spark.shuffle.service.enabled" → "true",
    "spark.dynamicAllocation.enabled" → "true"
  )
}
