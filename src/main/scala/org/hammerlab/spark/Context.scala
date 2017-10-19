package org.hammerlab.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.hammerlab.hadoop.Configuration

/**
 * [[SparkContext]]-wrapper that is also a [[Configuration Hadoop configuration object]] (and [[Serializable]]).
 */
case class Context(@transient sc: SparkContext)
  extends Configuration(sc.hadoopConfiguration)

object Context {
  implicit def makeContext(sc: SparkContext): Context = Context(sc)
  implicit def umakeContext(context: Context): SparkContext = context.sc

  def apply()(implicit conf: SparkConf): Context =
    Context(
      new SparkContext(
        conf
      )
    )
}
