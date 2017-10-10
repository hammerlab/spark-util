package org.hammerlab.spark

/*
import org.apache.spark.SparkEnv
import org.hammerlab.spark.Bar.sc
import org.hammerlab.test.Suite

class SelfRegistrarTest
  extends Suite
    with SelfRegistrar {
  test("foo") {
    val sc = Bar.sc
    sc.getConf.get("spark.kryo.registrator", "") should be("org.hammerlab.spark.Bar$")
    val ser = SparkEnv.get.serializer.newInstance()
    println(ser)

    val bar = Bar.getClass.newInstance
    println(bar)
  }

  override def afterAll(): Unit = {
    // Do this before the super delegation, which will remove the temporary event-log dir
    if (sc != null)
      sc.stop()

    super.afterAll()
  }
}

object Bar
  extends SparkConfBase
    with SelfRegistrar {
  sparkConf(
    "spark.master" → s"local[4]",
    "spark.app.name" → getClass.getName,
    "spark.driver.host" → "localhost"
  )
  implicit val conf = makeSparkConf
  val sc = Context()
}
*/
