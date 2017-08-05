package org.hammerlab.spark.confs

import org.apache.spark.serializer.{ KryoRegistrator, KryoSerializer }
import org.hammerlab.spark.SparkConfBase

trait Kryo {
  self: SparkConfBase ⇒

  def registrationRequired: Boolean = true
  def referenceTracking: Boolean = false
  def registrar: Class[_ <: KryoRegistrator] = null

  sparkConf(
    "spark.serializer" → classOf[KryoSerializer].getCanonicalName,
    "spark.kryo.referenceTracking" → referenceTracking.toString,
    "spark.kryo.registrationRequired" → registrationRequired.toString
  )

  Option(registrar)
    .foreach(
      clz ⇒
        sparkConf(
          "spark.kryo.registrator" → clz.getCanonicalName
        )
    )
}
