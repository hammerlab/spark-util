package org.hammerlab.spark.accumulator

import org.hammerlab.spark.ContextSuite
import org.hammerlab.kryo.cls

class HistogramTest
  extends ContextSuite {

  register(
    cls[Range]
  )

  test("count") {
    val histogram = Histogram[Int]("ones-digits")
    sc
      .parallelize(
        1 to 100,
        numSlices = 4
      )
      .map {
        i ⇒
          histogram.add(i % 10)
          i
      }
      .count should be(100)

    histogram.value.toList should be(0 to 9 map(_ → 10))
  }
}
