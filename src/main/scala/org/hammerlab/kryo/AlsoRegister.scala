package org.hammerlab.kryo

trait AlsoRegister[T] {
  def registrations: Seq[Registration]
}

object AlsoRegister {
  def apply[T](regs: Registration*): AlsoRegister[T] =
    new AlsoRegister[T] {
      override def registrations: Seq[Registration] = regs
    }
}
