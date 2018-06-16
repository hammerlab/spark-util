package org.hammerlab.kryo

import com.esotericsoftware.kryo
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.kryo.spark.Registrator

import scala.reflect.ClassTag

/**
 * Base for (conveniently, implicitly) performing kryo-registrations given various kinds of inputs.
 *
 * [[Class]] instances are the default / base case (and can come with an attendant implicit [[Serializer]] and
 * related/required registrations via the [[AlsoRegister]] type-class), but [[Registrar]]s and [[Registrator]]s
 * can be composed seamlessly as well for easy reuse of groups of registrations.
 */
sealed trait Registration {
  def apply(implicit kryo: Kryo): Unit
}

/**
 * Wrapper for a [[Class]] that should be directly kryo-registered, along with an [[Array]]-wrapped counterpart.
 */
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
   * Register a class, along with an optional custom serializer, list of classes that should also be registered, and
   * flag for registering an [[Array]]-ified class as well.
   */
  case class ClassWithSerializerToRegister[U](cls: Class[U],
                                              serializer: Option[Serializer[U]],
                                              alsoRegister: Option[AlsoRegister[U]],
                                              withArray: Boolean = false)
    extends Registration {
    override def apply(implicit kryo: Kryo): Unit = {
      serializer match {
        case Some(serializer) ⇒ kryo.register(cls, serializer(kryo))
        case _ ⇒ kryo.register(cls)
      }

      alsoRegister.foreach { _.registrations.foreach(_(kryo)) }

      if (withArray)
        kryo.register(ClassTag(cls).wrap.runtimeClass)
    }
  }

  /**
   * Compose all of a given [[Registrar]]'s [[Registration]]'s into a single [[Registration]], e.g. for re-use in a new
   * [[Registrar]].
   */
  implicit class RegistrarToRegister(registrar: Registrar)
    extends Registration {
    override def apply(implicit kryo: Kryo): Unit = registrar(kryo)
  }

  /**
   * Same as [[RegistrarToRegister]] but for a [[KryoRegistrator]].
   */
  implicit class KryoRegistratorToRegister(registrator: KryoRegistrator) extends Registration {
    override def apply(implicit kryo: Kryo): Unit = registrator.registerClasses(kryo)
  }

  /**
   * Same as [[RegistrarToRegister]] but with a [[Registrator]] (which is both a [[Registrar]] and a
   * [[KryoRegistrator]]).
   */
  implicit def registratorToRegistration(registrator: Registrator): Registration =
    RegistrarToRegister(registrator)

  /**
   * Convenience function, supports syntax like:
   *
   * register(classOf[Foo] → new FooSerializer)
   */
  implicit def withCustomKryoSerializer[U](
    t: (Class[U], kryo.Serializer[U])
  )(
    implicit alsoRegister: AlsoRegister[U] = null
  ):
    ClassWithSerializerToRegister[U] =
    ClassWithSerializerToRegister(
      t._1,
      Some(t._2),
      Option(alsoRegister)
    )

  implicit def withCustomSerializer[U](
    t: (Class[U], Serializer[U])
  )(
    implicit alsoRegister: AlsoRegister[U] = null
  ):
    ClassWithSerializerToRegister[U] =
    ClassWithSerializerToRegister(
      t._1,
      Some(t._2),
      Option(alsoRegister)
    )

  implicit def withCustomSerializerFn[U](
    t: (Class[U], Kryo ⇒ kryo.Serializer[U])
  )(
    implicit alsoRegister: AlsoRegister[U] = null
  ):
    ClassWithSerializerToRegister[U] =
    ClassWithSerializerToRegister(
      t._1,
      Some(t._2),
      Option(alsoRegister)
    )

  /**
   * Simple [[Registration]]-creation from a [[Class]].
   */
  implicit def classWithImplicits[U](cls: Class[U])(
      implicit
      serializer: Serializer[U] = null,
      alsoRegister: AlsoRegister[U] = null
  ):
    ClassWithSerializerToRegister[U] =
    ClassWithSerializerToRegister(
      cls,
      Option(serializer),
      Option(alsoRegister)
    )

  /**
   * Create a [[Registration]] from a [[ClassAndArray]].
   */
  implicit def classWithImplicitsAndArray[U](cls: ClassAndArray[U])(
    implicit
    serializer: Serializer[U] = null,
    alsoRegister: AlsoRegister[U] = null
  ):
    ClassWithSerializerToRegister[U] =
    ClassWithSerializerToRegister(
      cls,
      Option(serializer),
      Option(alsoRegister),
      withArray = true
    )

  /**
   * Create a [[Registration]] from a [[String]] ([[Class]] name).
   */
  implicit def classNameWithImplicits(
    className: String
  ):
    ClassWithSerializerToRegister[_] =
    ClassWithSerializerToRegister(
      Class.forName(className),
      None,
      None
    )
}
