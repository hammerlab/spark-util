package org.hammerlab.spark

import hammerlab.Suite
import org.apache.spark.SparkContext

class ContextTest
  extends Suite
     with ConfSuite {
  implicit val sc = new SparkContext(conf)

  def withContext(implicit ctx: Context) = {}

  test("derive") {
    // exercise implicit conversion and derivation from SparkContext
    withContext(sc)
    withContext
  }

  override protected def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }
}
