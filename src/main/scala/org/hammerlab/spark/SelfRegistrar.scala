package org.hammerlab.spark

import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.kryo.Registrar
import org.hammerlab.kryo.spark.Registrator

trait SelfRegistrar
  extends Registrator
    with confs.Kryo {
  registrar(this)
}
