package org.hammerlab.spark

import org.apache.spark.serializer.KryoSerializer
import org.hammerlab.kryo.spark.Registrator
import org.hammerlab.test.Suite

import scala.collection.mutable

class ConfsTest
  extends Suite
    with SparkConfBase
    with Registrator
    with confs.Kryo
    with confs.DynamicAllocation
    with confs.EventLog
    with confs.Speculation {

  sparkConf(
    "spark.master" → s"local[4]",
    "spark.app.name" → this.getClass.getName,
    "spark.driver.host" → "localhost"
  )

  override def registrar = this.getClass

  register(
    classOf[Array[String]],
    classOf[mutable.WrappedArray.ofRef[_]],
    classOf[Foo]
  )

  override lazy val eventLogDir = tmpDir()

  var sc: Context = _

  test("make SparkContext") {

    implicit val conf = makeSparkConf

    conf.get("spark.serializer") should be(classOf[KryoSerializer].getCanonicalName)
    conf.get("spark.dynamicAllocation.enabled") should be("true")
    conf.get("spark.eventLog.enabled") should be("true")
    conf.get("spark.eventLog.dir") should be(eventLogDir.toString)
    conf.get("spark.speculation") should be("true")

    sc = Context()
    val strings = Array("a", "b", "c", "d")
    val fooBroadcast = sc.broadcast(Foo("x"))
    val rdd = sc.parallelize(strings)
    rdd.count should be(4)
    rdd.map(_ + fooBroadcast.value.s).collect should be(Array("ax", "bx", "cx", "dx"))
  }

  override def afterAll(): Unit = {
    // Do this before the super delegation, which will remove the temporary event-log dir
    if (sc != null)
      sc.stop()

    super.afterAll()
  }
}

case class Foo(s: String)
