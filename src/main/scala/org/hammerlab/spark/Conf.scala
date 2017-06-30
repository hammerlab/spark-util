package org.hammerlab.spark

import org.apache.spark.SparkConf
import org.hammerlab.paths.Path

object Conf {
  def apply(propertiesFiles: String): SparkConf = {
    val cliSparkPropertiesFiles =
      propertiesFiles
        .split(",")
        .filterNot(_.isEmpty)

    val envSparkPropertiesFiles =
      Option(System.getenv("SPARK_PROPERTIES_FILES"))
        .toList
        .flatMap(_.split(","))
        .filterNot(_.isEmpty)

    // CLI props come after (and thus overwrite) env props
    val sparkPropertiesFiles =
      envSparkPropertiesFiles ++
        cliSparkPropertiesFiles

    val sparkProperties =
      sparkPropertiesFiles
        .flatMap {
          path ⇒
            Path(path)
              .lines
              .filter(_.trim.nonEmpty)
              .map {
                line ⇒
                  """(\S+)\s+(.*)""".r.findFirstMatchIn(line) match {
                    case Some(m) ⇒
                      m.group(1) → m.group(2)
                    case None ⇒
                      throw new IllegalArgumentException(s"Invalid property line in $path: '$line'")
                  }
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
