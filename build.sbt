name := "spark-util"

version := "1.2.0-SNAPSHOT"

deps += paths % "1.1.1-SNAPSHOT"

providedDeps += spark

testDeps := Seq(testUtils % "1.2.4-SNAPSHOT")
