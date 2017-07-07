package org.hammerlab.spark

import org.apache.spark
import org.apache.spark.rdd.RDD

/**
 * Generate a Spark [[org.apache.spark.Partitioner]] that maps elements to a partition indicated by an [[Int]] that
 * either is the key, or is the first element of a tuple.
 */
object KeyPartitioner {

  val pf: PartialFunction[Any, PartitionIndex] = {
    case i: Int                 => i
    case Product2(k: Int, _)    => k
    case Product3(k: Int, _, _) => k
  }

  def apply(rdd: RDD[_]): spark.Partitioner =
    apply(rdd.getNumPartitions)

  def apply(numPartitions: NumPartitions): spark.Partitioner =
    Partitioner(numPartitions, pf)
}
