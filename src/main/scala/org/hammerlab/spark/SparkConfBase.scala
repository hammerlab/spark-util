package org.hammerlab.spark

import org.apache.spark.SparkConf

import scala.collection.mutable

trait SparkConfBase {
  private val _sparkConfs = mutable.Map[String, String]()

  protected def sparkConfs: Map[String, String] = _sparkConfs.toMap

  protected def makeSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    for {
      (k, v) ← _sparkConfs
    } {
      sparkConf.set(k, v)
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
