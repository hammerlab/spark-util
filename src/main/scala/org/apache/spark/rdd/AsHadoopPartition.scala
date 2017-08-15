package org.apache.spark.rdd

import org.apache.spark.Partition
import org.hammerlab.hadoop.splits.FileSplit
import org.apache.hadoop.mapreduce.lib.input
import org.apache.hadoop.mapred

/**
 * Hack around [[org.apache.spark.rdd.NewHadoopPartition]] being private[spark]
 */
object AsHadoopPartition {
  def apply(partition: Partition): HadoopPartition =
    partition.asInstanceOf[HadoopPartition]
}

object AsNewHadoopPartition {
  def apply(partition: Partition): NewHadoopPartition =
    partition.asInstanceOf[NewHadoopPartition]
}

object GetFileSplit {
  def apply(partition: Partition): FileSplit =
    partition match {
      case p: HadoopPartition ⇒
        FileSplit(
          p
            .inputSplit
            .value
            .asInstanceOf[mapred.FileSplit]
        )
      case p: NewHadoopPartition ⇒
        FileSplit(
          p
            .serializableHadoopSplit
            .value
            .asInstanceOf[input.FileSplit]
        )
      case _ ⇒
        throw NonHadoopPartition(partition)
    }
}

case class NonHadoopPartition(p: Partition)
  extends IllegalArgumentException
