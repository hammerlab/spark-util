name := "spark-util"
r"2.0.1"
dep(
  paths % "1.3.1",
  hadoop provided,
  kryo,
  spark provided
)
