package org.hammerlab.kryo

/**
 * Type-class for expressing dependent kryo-registrations that should go along with a given type [[T]], e.g. when
 * [[T]]'s serialization/deserialization requires serde of other types.
 */
trait AlsoRegister[T] {
  def registrations: Seq[Registration]
}

object AlsoRegister {
  def apply[T](regs: Registration*): AlsoRegister[T] =
    new AlsoRegister[T] {
      override def registrations: Seq[Registration] = regs
    }
}
