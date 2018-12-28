package org.hammerlab.spark

import org.apache.spark.SparkContext
import org.hammerlab.test.Suite

trait ConfSuite
  extends SparkConfBase {
  implicit lazy val conf = makeSparkConf
  sparkConf(
    "spark.master" → s"local[4]",
    "spark.app.name" → getClass.getName,
    "spark.driver.host" → "localhost",
    "spark.kryo.classesToRegister" → "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"
  )
}

abstract class ContextSuite
  extends Suite
    with ConfSuite
    with SelfRegistrar {

  private var _sc: Context = _
  implicit lazy val sc = {
    _sc = Context()
    _sc
  }
  implicit def sparkContext: SparkContext = sc

  override def afterAll(): Unit = {
    // Do this before the super delegation, which will remove the temporary event-log dir
    if (_sc != null)
      _sc.stop()

    super.afterAll()
  }
}
