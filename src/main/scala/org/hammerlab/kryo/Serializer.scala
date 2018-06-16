package org.hammerlab.kryo

import com.esotericsoftware.kryo
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ Input, Output }

sealed trait Serializer[T] {
  def apply(k: Kryo): kryo.Serializer[T]
}

object Serializer {
  implicit class Kryo[T](serializer: kryo.Serializer[T]) extends Serializer[T] {
    override def apply(k: kryo.Kryo): kryo.Serializer[T] = serializer
  }
  implicit class Fn[T](fn: kryo.Kryo ⇒ kryo.Serializer[T]) extends Serializer[T] {
    override def apply(k: kryo.Kryo): kryo.Serializer[T] = fn(k)
  }

  def apply[T](r: (kryo.Kryo, Input) ⇒ T, w: (kryo.Kryo, Output, T) ⇒ Unit): Serializer[T] =
    new kryo.Serializer[T] {
      override def read(k: kryo.Kryo, input: Input, `type`: Class[T]): T = r(k, input)
      override def write(k: kryo.Kryo, output: Output, t: T): Unit = w(k, output, t)
    }

  def apply[T](fn: kryo.Kryo ⇒ kryo.Serializer[T]): Serializer[T] = fn
}
