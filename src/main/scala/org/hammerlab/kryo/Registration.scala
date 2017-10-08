package org.hammerlab.kryo

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.kryo.spark.Registrator

import scala.reflect.ClassTag

/**
 * Base for (usually implicitly-created) kryo-registrations that subclasses can add.
 */
sealed trait Registration {
  def apply(implicit kryo: Kryo): Unit
}

case class ClassAndArray[T](cls: Class[T])
object ClassAndArray {
  def apply[T](implicit ct: ClassTag[T]): ClassAndArray[T] =
    ClassAndArray(
      ct.runtimeClass.asInstanceOf[Class[T]]
    )

  implicit def unwrap[T](cls: ClassAndArray[T]): Class[T] = cls.cls
}

object Registration {

  /**
   * Register a class, along with a custom serializer.
   */
  case class ClassWithSerializerToRegister[U](cls: Class[U],
                                              serializer: Option[Serializer[U]],
                                              alsoRegister: Option[AlsoRegister[U]],
                                              withArray: Boolean = false)
    extends Registration {
    override def apply(implicit kryo: Kryo): Unit = {
      serializer match {
        case Some(serializer) ⇒ kryo.register(cls, serializer)
        case _ ⇒ kryo.register(cls)
      }

      alsoRegister.foreach { _.registrations.foreach(_(kryo)) }

      if (withArray)
        kryo.register(ClassTag(cls).wrap.runtimeClass)
    }
  }

  /**
   * Compose all of a provided [[Registrar]]'s [[Registration]]'s with this [[Registrar]].
   */
  implicit class RegistrarToRegister(registrar: Registrar)
    extends Registration {
    override def apply(implicit kryo: Kryo): Unit = registrar(kryo)
  }

  /**
   * Compose all of a provided [[KryoRegistrator]]'s [[Registration]]'s with this [[Registrar]].
   */
  implicit class KryoRegistratorToRegister(registrator: KryoRegistrator) extends Registration {
    override def apply(implicit kryo: Kryo): Unit = registrator.registerClasses(kryo)
  }

  implicit def registratorToRegistration(registrator: Registrator): Registration =
    RegistrarToRegister(registrator)

  /**
   * Convenience function, supports syntax like:
   *
   * register(classOf[Foo] → new FooSerializer)
   */
  implicit def withCustomSerializer[U](t: (Class[U], Serializer[U]))(
      implicit alsoRegister: AlsoRegister[U] = null
  ): ClassWithSerializerToRegister[U] =
    ClassWithSerializerToRegister(
      t._1,
      Some(t._2),
      Option(alsoRegister)
    )

  implicit def classWithImplicits[U](cls: Class[U])(
      implicit
      serializer: Serializer[U] = null,
      alsoRegister: AlsoRegister[U] = null
  ): ClassWithSerializerToRegister[U] =
    ClassWithSerializerToRegister(
      cls,
      Option(serializer),
      Option(alsoRegister)
    )

  implicit def classWithImplicitsAndArray[U](cls: ClassAndArray[U])(
      implicit
      serializer: Serializer[U] = null,
      alsoRegister: AlsoRegister[U] = null
  ): ClassWithSerializerToRegister[U] =
    ClassWithSerializerToRegister(
      cls,
      Option(serializer),
      Option(alsoRegister),
      withArray = true
    )

  implicit def classNameWithImplicits(className: String): ClassWithSerializerToRegister[_] =
    ClassWithSerializerToRegister(
      Class.forName(className),
      None,
      None
    )
}
