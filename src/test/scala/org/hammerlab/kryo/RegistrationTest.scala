package org.hammerlab.kryo

import com.esotericsoftware.kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.kryo.spark.Registrator
import org.hammerlab.kryo.spark.Registrator._
import org.hammerlab.spark.{ Context, ContextSuite }

import scala.collection.mutable

class RegistrationTest
  extends ContextSuite {

  register(
    "org.hammerlab.kryo.A",
    classOf[Foo],
    classOf[Array[Foo]],
    classOf[mutable.WrappedArray.ofRef[_]],
    classOf[B] → BSerializer,
    CDRegistrar,
    CDRegistrar: KryoRegistrator,  // test duplicate registration and a Registrator implicit
    new EFRegistrator
  )

  test("registrations") {
    implicit val conf = makeSparkConf
    sc = Context()

    sc
      .parallelize(
        Array(
          Foo(A( 10), B( 20), C( 30), D( 40), E( 50), F( 60)),
          Foo(A(100), B(200), C(300), D(400), E(500), F(600))
        ),
        numSlices = 2
      )
      .flatMap {
        case Foo(A(a), B(b), C(c), D(d), E(e), F(f)) ⇒
          Array(
            a,
            b,
            c,
            d,
            e,
            f
          )
      }
      .collect should be(
      Array(
        10,
        200,
        30,
        40,
        50,
        60,
        100,
        2000,
        300,
        400,
        500,
        600
      )
    )
  }
}

/** Dummy serializer that 10x's the value of [[B]] instances passed through it */
object BSerializer extends Serializer[B] {
  override def read(k: kryo.Kryo, input: Input, clz: Class[B]): B = B(input.readInt())
  override def write(k: kryo.Kryo, output: Output, b: B) = output.writeInt(b.n * 10)
}

object CDRegistrar extends Registrar {
  register(
    classOf[C],
    classOf[D]
  )
}

class EFRegistrator extends KryoRegistrator {
  override def registerClasses(k: kryo.Kryo): Unit = {
    k.register(classOf[E])
    k.register(classOf[F])
  }
}

case class Foo(a: A, b: B, c: C, d: D, e: E, f: F)
case class A(n: Int)
case class B(n: Int)
case class C(n: Int)
case class D(n: Int)
case class E(n: Int)
case class F(n: Int)
