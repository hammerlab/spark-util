package org.hammerlab.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.test.Suite

class KryoConfsTest
  extends Suite {
  test("override registration requirement") {
    val conf = HasSparkConf.conf
    conf.get("spark.kryo.referenceTracking") should be("true")
    conf.get("spark.kryo.registrationRequired") should be("false")
    conf.get("spark.kryo.registrator") should be("org.hammerlab.spark.TestRegistrator")
  }
}

class TestRegistrator
  extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = ???
}

object HasSparkConf
  extends SparkConfBase
    with KryoConfs {
  val conf = makeSparkConf
  override def registrationRequired = false
  override def referenceTracking = true
  override def registrar = classOf[TestRegistrator]
}
