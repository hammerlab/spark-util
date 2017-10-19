package org.hammerlab.spark

import org.hammerlab.kryo.spark.Registrator

/**
 * Minimal trait for objects that can be used as their own [[org.apache.spark.serializer.KryoRegistrator]]s, for ease of
 * encapsulating Kryo-registration information in tandem with [[org.hammerlab.kryo.Registrar.register]] syntax.
 */
trait SelfRegistrar
  extends Registrator
    with confs.Kryo {
  registrar(this)
}
