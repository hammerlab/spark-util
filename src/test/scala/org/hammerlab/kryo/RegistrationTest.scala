package org.hammerlab.kryo

import com.esotericsoftware.kryo
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.serializers.FieldSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.kryo.spark.Registrator
import org.hammerlab.spark.ContextSuite

import scala.collection.mutable

class RegistrationTest
  extends ContextSuite {

  register(
    cls[A],  // comes with an AlsoRegister that loops in B and its implicit custom Serializer
    arr[Foo],
    cls[mutable.WrappedArray.ofRef[_]],
    CDRegistrar,
    CDRegistrar: KryoRegistrator,  // test duplicate registration and a Registrator implicit
    CDRegistrar: Registrar,        // test duplicate registration and a Registration implicit
    new EFRegistrator,
    cls[G],
    cls[H] → H.serializer,
    cls[I] → I.serializer
  )

  test("registrations") {
    sc
      .parallelize(
        Array(
          Foo(A( 10), B( 20), C( 30), D( 40), E( 50), F( 60), G( 70), H( 80), I( 90)),
          Foo(A(100), B(200), C(300), D(400), E(500), F(600), G(700), H(800), I(900))
        ),
        numSlices = 2
      )
      .flatMap {
        case Foo(A(a), B(b), C(c), D(d), E(e), F(f), G(g), H(h), I(i)) ⇒
          Array(
            a,
            b,
            c,
            d,
            e,
            f,
            g,
            h,
            i
          )
      }
      .collect should be(
      Array(
        10,
        200,
        30,
        42,
        50,
        60,
        70,
        80,
        90,
        100,
        2000,
        300,
        42,
        500,
        600,
        700,
        800,
        900
      )
    )
  }
}

// Wrapper for inducing/testing serde of a bunch of classes
case class Foo(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I)

case class A(n: Int)
object A {
  /** Registering [[A]] implicitly causes registration of [[B]] along with [[B]]'s implicit custom [[Serializer]] */
  implicit val alsoRegister: AlsoRegister[A] =
    AlsoRegister(classOf[B])
}

case class B(n: Int)
object B {
  /** Dummy [[Serializer]] that 10x's a value that is round-tripped through it, for testing/verification purposes */
  implicit val serializer: Serializer[B] =
    Serializer(
      (k, input) ⇒ B(input.readInt()),
      (k, output, b) ⇒ output.writeInt(b.n * 10)
    )
}


case class C(n: Int)

case class D(n: Int)
object DSerializer extends kryo.Serializer[D] {
  /** Dummy [[Serializer]] that sets all values to 42, for testing/verification purposes */
  override def read(kryo: Kryo, input: Input, cls: Class[D]): D = { input.readInt(); D(42) }
  override def write(kryo: Kryo, output: Output, d: D): Unit = output.writeInt(d.n)
}

/** Test composing [[Registration]]s by registering this [[Registrar]] */
object CDRegistrar extends Registrator {
  register(
    "org.hammerlab.kryo.C",   // test picking up classes by name
    classOf[D] → DSerializer  // test explicitly providing a custom serializer
  )
}

case class E(n: Int)
case class F(n: Int)

/** Test composing [[Registration]]s by registering this [[KryoRegistrator]] */
class EFRegistrator extends KryoRegistrator {
  override def registerClasses(k: kryo.Kryo): Unit = {
    k.register(classOf[E])
    k.register(classOf[F])
  }
}

case class G(n: Int)
object G {
  implicit val serializer =
    Serializer(
      new FieldSerializer[G](_, classOf[G])
    )
}

case class H(n: Int)
object H {
  // not implicit, registered explicitly above
  val serializer =
    Serializer(
      new FieldSerializer[H](_, classOf[H])
    )
}

case class I(n: Int)
object I {
  // not implicit, registered explicitly above
  val serializer =
    (kryo: Kryo) ⇒
      new FieldSerializer[I](kryo, classOf[I])
}
