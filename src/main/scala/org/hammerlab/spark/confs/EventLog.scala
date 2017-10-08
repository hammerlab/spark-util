package org.hammerlab.spark.confs

import java.nio.file.Files.createTempDirectory

import org.hammerlab.paths.Path
import org.hammerlab.spark.SparkConfBase

trait EventLog {
  self: SparkConfBase ⇒

  def listenerBusSize = 1000000
  private val defaultEventLogDir = Path("file:///tmp/spark-events")

  lazy val eventLogDir: Path =
    if (defaultEventLogDir.exists)
      defaultEventLogDir
    else
      Path(createTempDirectory("spark-events"))

  sparkConf(
    "spark.eventLog.enabled" → "true",
    "spark.eventLog.dir" → eventLogDir.toString,
    "spark.yarn.maxAppAttempts" → "1",
    "spark.scheduler.listenerbus.eventqueue.size" → listenerBusSize.toString
  )
}
