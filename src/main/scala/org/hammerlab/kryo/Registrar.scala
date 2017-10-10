package org.hammerlab.kryo

import com.esotericsoftware.kryo.Kryo

import scala.collection.mutable.ArrayBuffer

/**
 * Trait for encapsulating related sets of kryo-registrations.
 *
 * The `register` method supports a variable number of [[Registration]]s.
 *
 * Can be used directly as a [[org.apache.spark.serializer.KryoRegistrator]] (see [[spark.Registrator]] and
 * [[spark.Registrar]]) passed to a [[org.apache.spark.SparkConf]] / [[org.apache.spark.SparkContext]], or can
 * be mixed-in to e.g. companion objects of classes to record kryo-registrations required by the Spark operations
 * in the corresponding class, and composed with downstream [[Registrar]]s (see implicit conversion to
 * [[Registration]] via [[Registration.registratorToRegistration]]) to track and reuse relevant groups of
 * registrations.
 */
trait Registrar {
  def apply(implicit kryo: Kryo): Unit =
    extraKryoRegistrations.foreach(_(kryo))

  /**
   * Additional registrations are queued here during instance initialization.
   */
  private val extraKryoRegistrations = ArrayBuffer[Registration]()

  def register(registrations: Registration*): Unit =
    extraKryoRegistrations ++= registrations
}
