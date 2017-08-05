package org.hammerlab.hadoop.kryo

import java.io.{ ObjectInputStream, ObjectOutputStream }

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }

case class SerializableSerializer[T <: Serializable]()
  extends Serializer[T] {
  override def read(kryo: Kryo, input: Input, `type`: Class[T]): T =
    new ObjectInputStream(input)
      .readObject()
      .asInstanceOf[T]

  override def write(kryo: Kryo, output: Output, t: T): Unit =
    new ObjectOutputStream(output)
      .writeObject(t)
}
