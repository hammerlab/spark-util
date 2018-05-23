package org.hammerlab.spark

import org.apache.spark.SparkConf

import scala.collection.mutable

/**
 * Interface for applications to register fall-back default Spark-configuration values, using the
 * [[SparkConfBase]] `sparkConf` method below.
 *
 * Configs are added to a [[org.apache.spark.SparkConf]] after it's been instantiated and other defaults have been
 * applied to it, and are only written to keys that have no value.
 */
trait SparkConfBase {
  private val _sparkConfs = mutable.Map[String, String]()

  protected def sparkConfs: Map[String, String] = _sparkConfs.toMap

  protected def makeSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    for {
      (k, v) ← _sparkConfs
    } {
      sparkConf.setIfMissing(k, v)
    }
    sparkConf
  }

  protected def sparkConf(confs: (String, String)*): Unit =
    for {
      (k, v) ← confs
    } {
      _sparkConfs(k) = v
    }
}
