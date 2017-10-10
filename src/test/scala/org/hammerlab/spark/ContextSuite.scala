package org.hammerlab.spark

import org.hammerlab.test.Suite

abstract class ContextSuite
  extends Suite
    with SparkConfBase
    with SelfRegistrar {

  var sc: Context = _

  sparkConf(
    "spark.master" → s"local[4]",
    "spark.app.name" → getClass.getName,
    "spark.driver.host" → "localhost"
  )

  override def afterAll(): Unit = {
    // Do this before the super delegation, which will remove the temporary event-log dir
    if (sc != null)
      sc.stop()

    super.afterAll()
  }
}
