package org.hammerlab.spark

import org.apache.spark.serializer.KryoSerializer

import scala.collection.mutable

class ConfsTest
  extends ContextSuite
    with confs.DynamicAllocation
    with confs.EventLog
    with confs.Speculation {

  val eventLogDir = tmpDir().toString

  sparkConf(
    "spark.eventLog.dir" → eventLogDir
  )

  register(
    classOf[Array[String]],
    classOf[mutable.WrappedArray.ofRef[_]],
    classOf[Foo]
  )

  test("make SparkContext") {

    implicit val conf = makeSparkConf

    conf.get("spark.serializer") should be(classOf[KryoSerializer].getCanonicalName)
    conf.get("spark.dynamicAllocation.enabled") should be("true")
    conf.get("spark.eventLog.enabled") should be("true")
    conf.get("spark.eventLog.dir") should be(eventLogDir)
    conf.get("spark.speculation") should be("true")

    sc = Context()
    val strings = Array("a", "b", "c", "d")
    val fooBroadcast = sc.broadcast(Foo("x"))
    val rdd = sc.parallelize(strings)
    rdd.count should be(4)
    rdd.map(_ + fooBroadcast.value.s).collect should be(Array("ax", "bx", "cx", "dx"))
  }
}

case class Foo(s: String)
