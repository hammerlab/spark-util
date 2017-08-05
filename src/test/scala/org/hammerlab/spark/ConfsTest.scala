package org.hammerlab.spark

import org.apache.spark.serializer.KryoSerializer
import org.hammerlab.test.Suite

class ConfsTest
  extends Suite
    with SparkConfBase
    with confs.Kryo
    with confs.DynamicAllocation
    with confs.EventLog
    with confs.Speculation {
  test("make SparkContext") {
    val conf = makeSparkConf

    conf.get("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    conf.get("spark.dynamicAllocation.enabled") should be("true")
    conf.get("spark.eventLog.enabled") should be("true")
    conf.get("spark.speculation", "true")
  }
}
