package org.hammerlab.spark

import org.hammerlab.test.Suite

class KeyPartitionerTest
  extends Suite {
  test("types") {
    KeyPartitioner(456).getPartition(123) should be(123)
    KeyPartitioner(456).getPartition(123 → "abc") should be(123)
    intercept[UnexpectedKey] {
      KeyPartitioner(456).getPartition("abc")
    }.key should be("abc")
  }

  test("partitioner") {
    val partitioner =
      Partitioner[Either[Int, String]](
        2,
        {
          case Left(n) ⇒ 0
          case Right(str) ⇒ 1
        }
      )

    partitioner.getPartition(Left(222)) should be(0)
    partitioner.getPartition(Right("abc")) should be(1)

    intercept[UnexpectedKey] {
      partitioner.getPartition(333)
    }.key should be(333)

    intercept[UnexpectedKey] {
      partitioner.getPartition("ddd")
    }.key should be("ddd")
  }
}
