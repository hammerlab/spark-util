package org.hammerlab.kryo

import com.esotericsoftware.kryo.Kryo

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait Registrar {
  def apply(implicit kryo: Kryo): Unit =
    extraKryoRegistrations.foreach(_(kryo))

  /**
   * Additional registrations are queued here during instance initialization.
   */
  private val extraKryoRegistrations = ArrayBuffer[Registration]()

  def register(registrations: Registration*): Unit =
    extraKryoRegistrations ++= registrations

  def cls[T](implicit ct: ClassTag[T]): Class[T] = ct.runtimeClass.asInstanceOf[Class[T]]
  def arr[T](implicit ct: ClassTag[T]): ClassAndArray[T] = ClassAndArray[T]
}

object Registrar {
  /**
   * Record a sequence of [[Registration]]s for later registration with Kryo, during [[org.apache.spark.SparkEnv]]
   * initialization.
   */
  def register(registrations: Registration*)(implicit registrar: Registrar): Unit =
    registrar.register(registrations: _*)
}
