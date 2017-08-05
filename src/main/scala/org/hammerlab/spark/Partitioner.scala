package org.hammerlab.spark

import org.apache.spark

object Partitioner {
  def apply(num: NumPartitions,
            pf: PartialFunction[Any, PartitionIndex]): spark.Partitioner =
    new spark.Partitioner {
      override def numPartitions: PartitionIndex = num
      override def getPartition(key: Any): PartitionIndex =
        if (pf.isDefinedAt(key))
          pf(key)
        else
          throw UnexpectedKey(key)
    }

  def apply[T](num: NumPartitions,
               fn: T ⇒ PartitionIndex): spark.Partitioner =
    new spark.Partitioner {
      override def numPartitions: PartitionIndex = num
      override def getPartition(key: Any): PartitionIndex =
        try {
          fn(key.asInstanceOf[T])
        } catch {
          case _: ClassCastException ⇒
            throw UnexpectedKey(key)
        }
    }
}

case class UnexpectedKey(key: Any)
  extends AssertionError(s"Key: $key")
