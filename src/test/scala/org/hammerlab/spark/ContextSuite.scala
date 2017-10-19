package org.hammerlab.spark

import org.apache.spark.SparkContext
import org.hammerlab.test.Suite

abstract class ContextSuite
  extends Suite
    with SparkConfBase
    with SelfRegistrar {

  implicit lazy val conf = makeSparkConf

  private var _sc: Context = _
  implicit lazy val sc = {
    _sc = Context()
    _sc
  }
  implicit def sparkContext: SparkContext = sc

  sparkConf(
    "spark.master" → s"local[4]",
    "spark.app.name" → getClass.getName,
    "spark.driver.host" → "localhost"
  )

  override def afterAll(): Unit = {
    // Do this before the super delegation, which will remove the temporary event-log dir
    if (_sc != null)
      _sc.stop()

    super.afterAll()
  }
}
