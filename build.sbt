name := "spark-util"

version := "2.0.0-SNAPSHOT"

deps ++= Seq(
  paths % "1.3.1",
  kryo
)

providedDeps ++= Seq(
  spark,
  hadoop
)

testDeps := Seq(testUtils)
