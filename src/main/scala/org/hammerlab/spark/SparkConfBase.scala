package org.hammerlab.spark

import java.lang.System.setProperty

import org.apache.spark.SparkConf

import scala.collection.mutable

trait SparkConfBase {
  private val sparkConfs = mutable.Map[String, String]()

  val overrideDefaults = false

  protected def makeSparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    for {
      (k, v) ← sparkConfs
    } {
      if (overrideDefaults)
        sparkConf.set(k, v)
      else
        sparkConf.setIfMissing(k, v)
    }
    sparkConf
  }

  protected def setSparkProps(): Unit =
    for {
      (k, v) ← sparkConfs
    } {
      setProperty(k, v)
    }

  protected def sparkConf(confs: (String, String)*): Unit =
    for {
      (k, v) ← confs
    } {
      sparkConfs(k) = v
    }
}
