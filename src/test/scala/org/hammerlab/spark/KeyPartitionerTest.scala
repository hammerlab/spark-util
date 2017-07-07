package org.hammerlab.spark

import org.hammerlab.test.Suite

class KeyPartitionerTest
  extends Suite {
  test("types") {
    KeyPartitioner(456).getPartition(123) should be(123)
    KeyPartitioner(456).getPartition(123 → "abc") should be(123)
  }
}
