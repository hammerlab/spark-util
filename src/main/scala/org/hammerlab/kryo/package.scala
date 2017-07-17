package org.hammerlab

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }

package object kryo {
  def serializeAs[T, U](implicit to: T ⇒ U, from: U ⇒ T): Serializer[T] =
    new Serializer[T] {
      override def read(kryo: Kryo, input: Input, `type`: Class[T]): T =
        from(
          kryo
            .readClassAndObject(input)
            .asInstanceOf[U]
        )

      override def write(kryo: Kryo, output: Output, t: T): Unit =
        kryo.writeClassAndObject(output, to(t))
    }
}
