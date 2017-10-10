package org.hammerlab.kryo.spark

import org.hammerlab.kryo.Registration

/**
 * Add a handy constructor to [[Registrar]]; easiest way to declare a [[org.apache.spark.serializer.KryoRegistrator]]
 * for use in Spark apps, and use convenient [[Registration]] / [[org.hammerlab.kryo.Registrar]] APIs for listing /
 * composing registered classes.
 */
abstract class Registrar(registrations: Registration*)
  extends Registrator {
  register(registrations: _*)
}
