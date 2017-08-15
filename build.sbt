name := "spark-util"

version := "1.3.0"

deps += paths % "1.2.0"

providedDeps += spark

testDeps := Seq(testUtils)
