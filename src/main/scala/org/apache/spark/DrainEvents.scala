package org.apache.spark

object DrainEvents {
  def apply(delayMs: Int = 30000)(implicit sc: SparkContext): Unit = {
    sc.listenerBus.waitUntilEmpty(delayMs)
  }
}
