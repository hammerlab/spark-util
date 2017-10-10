name := "spark-util"

version := "2.0.0"

deps ++= Seq(
  paths % "1.3.1",
  kryo
)

providedDeps ++= Seq(
  hadoop,
  spark
)

testDeps := Seq(testUtils)
