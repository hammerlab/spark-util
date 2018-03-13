name := "spark-util"
v"2.0.3"
scala211Only
dep(
  paths % "1.5.0",
  hadoop provided,
  kryo,
  spark provided
)
