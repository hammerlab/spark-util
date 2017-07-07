name := "spark-util"

version := "1.2.0-SNAPSHOT"

deps += libs.value('paths).copy(revision = "1.1.1-SNAPSHOT")

testUtilsVersion := "1.2.4-SNAPSHOT"

providedDeps += spark.value
