name := "spark-util"
v"2.0.5"
`2.11`.only
dep(
  paths % "1.5.0",
  hadoop provided,
  kryo,
  spark provided
)
