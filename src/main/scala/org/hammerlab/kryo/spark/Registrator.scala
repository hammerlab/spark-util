package org.hammerlab.kryo.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.kryo

/**
 * Fold [[Registrar]] interface into Spark's [[KryoRegistrator]] API.
 */
trait Registrator
  extends KryoRegistrator
    with kryo.Registrar {
  override def registerClasses(kryo: Kryo): Unit =
    this.apply(kryo)
}

object Registrator {
  implicit def registrarToRegistrator(registrar: Registrar): KryoRegistrator =
    new KryoRegistrator {
      override def registerClasses(kryo: Kryo): Unit =
        registrar(kryo)
    }
}
