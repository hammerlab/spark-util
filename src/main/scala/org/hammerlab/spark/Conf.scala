package org.hammerlab.spark

import org.apache.spark.SparkConf
import org.hammerlab.paths.Path

object Conf {

  val propsLineRegex = """(\S+)\s+(.*)""".r

  def apply(loadDefaults: Boolean = true): SparkConf = {
    val envSparkPropertiesFiles =
      Option(System.getenv("SPARK_PROPERTIES_FILES"))
        .toList
        .flatMap(_.split(","))
        .filterNot(_.isEmpty)

    val sparkProperties =
      envSparkPropertiesFiles
        .flatMap {
          path ⇒
            Path(path)
              .lines
              .filter(_.trim.nonEmpty)
              .map {
                case propsLineRegex(key, value) ⇒
                  key → value
                case line ⇒
                  throw new IllegalArgumentException(
                    s"Invalid property line in $path: '$line'"
                  )
              }
        }

    val sparkConf = new SparkConf()

    for {
      (k, v) ← sparkProperties
    } {
      sparkConf.set(k, v)
    }

    sparkConf
  }
}
