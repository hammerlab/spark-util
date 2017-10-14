name := "spark-util"

version := "2.0.1-SNAPSHOT"

deps ++= Seq(
  paths % "1.3.1",
  kryo
)

providedDeps ++= Seq(
  hadoop,
  spark
)

testDeps := Seq(testUtils)
