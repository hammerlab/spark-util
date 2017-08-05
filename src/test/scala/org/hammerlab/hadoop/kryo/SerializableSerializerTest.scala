package org.hammerlab.hadoop.kryo

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.hammerlab.test.Suite

class SerializableSerializerTest
  extends Suite {
  test("serde") {
    val kryo = new Kryo()
    kryo.setRegistrationRequired(true)
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos)

    val foo = new Foo
    foo.n = 123
    foo.s = "abc"

    intercept[IllegalArgumentException] {
      kryo.writeClassAndObject(output, foo)
    }
    .getMessage should startWith("Class is not registered: org.hammerlab.hadoop.kryo.Foo")

    kryo.register(classOf[Foo], SerializableSerializer[Foo]())

    kryo.writeClassAndObject(output, foo)

    output.close()

    val bytes = baos.toByteArray
    bytes.length should be(93)

    val bais = new ByteArrayInputStream(bytes)

    val input = new Input(bais)
    val after = kryo.readClassAndObject(input).asInstanceOf[Foo]

    after.n should be(foo.n)
    after.s should be(foo.s)
  }
}

class Foo
  extends Serializable {

  var n = 0
  var s = ""

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(n)
    out.writeUTF(s)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    n = in.readInt()
    s = in.readUTF()
  }
}
