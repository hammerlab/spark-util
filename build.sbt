name := "spark-util"

version := "1.1.3-SNAPSHOT"

deps += libs.value('paths)
providedDeps += spark.value
