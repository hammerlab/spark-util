package org.hammerlab.spark

import org.hammerlab.test.Suite

class ConfTest
  extends Suite {

  def write(str: String): String = {
    val path = tmpPath()
    path.write(str.stripMargin)
    path.toString()
  }

  test("files") {
    val f1 =
      write(
        """
          |spark.master local
          |a.b.c    foo
        """
      )

    val f2 =
      write(
        """d	e
          |
          |f		g h i
          |a.b.c  bar
        """
      )

    setEnv("SPARK_PROPERTIES_FILES", s"$f1,$f2")

    val conf = Conf()

    val expected =
      Seq(
        "spark.master" → "local",
        "a.b.c" → "bar",
        "d" → "e",
        "f" → "g h i"
      )

    for {
      (k, v) ← expected
    } {
      conf.getOption(k) should be(Some(v))
    }
  }
}
