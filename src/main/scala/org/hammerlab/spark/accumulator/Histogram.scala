package org.hammerlab.spark.accumulator

import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

import scala.collection.immutable.SortedMap
import scala.collection.mutable

/**
 * [[AccumulatorV2 Accumulator]] that keeps a counter of "key" objects that are added to it, ultimately emitting a
 * [[SortedMap]] of keys and their counts.
 */
case class Histogram[T: Ordering](var map: mutable.Map[T, Long] = mutable.Map.empty[T, Long])
  extends AccumulatorV2[T, SortedMap[T, Long]] {

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[T, SortedMap[T, Long]] =
    Histogram(map.clone())

  override def reset(): Unit = map = mutable.Map.empty[T, Long]

  override def add(k: T): Unit =
    map.update(
      k,
      map.getOrElse(k, 0L) + 1
    )

  override def merge(other: AccumulatorV2[T, SortedMap[T, Long]]): Unit =
    for {
      (k, v) ← other.value
    } {
      map.update(k, map.getOrElse(k, 0L) + v)
    }

  override def value: SortedMap[T, Long] = SortedMap(map.toSeq: _*)
}

object Histogram {
  def apply[T: Ordering](name: String)(implicit sc: SparkContext): Histogram[T] = {
    val a = Histogram[T]()
    sc.register(a, name)
    a
  }
}
