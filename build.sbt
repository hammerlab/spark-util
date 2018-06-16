name := "spark-util"
v"3.0.0"
`2.11`.only
dep(
  paths % "1.5.0",
  hadoop provided,
  kryo,
  spark provided
)
