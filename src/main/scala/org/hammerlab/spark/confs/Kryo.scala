package org.hammerlab.spark.confs

import org.apache.spark.serializer.{ KryoRegistrator, KryoSerializer }
import org.hammerlab.kryo.Registrar
import org.hammerlab.spark.SparkConfBase

import scala.reflect.ClassTag

case class UserRegistrar(name: String)

object UserRegistrar {

  implicit def fromInstance[T <: KryoRegistrator](t: T): UserRegistrar =
    UserRegistrar(t.getClass.getName)

  implicit def romClass[T <: KryoRegistrator](cls: Class[T]): UserRegistrar =
    UserRegistrar(cls.getName)

  implicit def fromClassTag[T <: KryoRegistrator](implicit ct: ClassTag[T]): UserRegistrar =
    UserRegistrar(ct.runtimeClass.getName)
}

trait Kryo
  extends SparkConfBase
    with Registrar {

  def registrationRequired: Boolean = true
  def referenceTracking: Boolean = false

  def registrar(userRegistrar: UserRegistrar): Unit =
    sparkConf(
      "spark.kryo.registrator" → userRegistrar.name
    )

  def registrar[T <: KryoRegistrator](implicit ct: ClassTag[T]): Unit =
    registrar(UserRegistrar.fromClassTag(ct))

  sparkConf(
    "spark.serializer" → classOf[KryoSerializer].getName,
    "spark.kryo.referenceTracking" → referenceTracking.toString,
    "spark.kryo.registrationRequired" → registrationRequired.toString
  )
}
