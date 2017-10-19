package org.hammerlab.hadoop.kryo

import java.io.{ DataInputStream, DataOutputStream }

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.apache.hadoop.io.Writable

/**
 * Kryo [[Serializer]] that wraps a Hadoop [[Writable]]
 */
class WritableSerializer[T <: Writable](ctorArgs: Any*)
  extends Serializer[T] {
  override def read(kryo: Kryo, input: Input, clz: Class[T]): T = {
    val t = clz.newInstance()
    t.readFields(new DataInputStream(input))
    t
  }

  override def write(kryo: Kryo, output: Output, t: T): Unit = {
    t.write(new DataOutputStream(output))
  }
}
