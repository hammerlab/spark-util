name := "spark-util"
v"2.0.4"
`2.11`.only
sonatypeStage(1457)
dep(
  paths % "1.5.0",
  hadoop provided,
  kryo,
  spark provided
)
