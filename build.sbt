name := "spark-util"

version := "1.2.0-SNAPSHOT"

deps += paths % "1.2.0"

providedDeps += spark

testDeps := Seq(testUtils % "1.3.0")
