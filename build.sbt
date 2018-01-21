name := "spark-util"
scala211Only
dep(
  paths % "1.4.0",
  hadoop provided,
  kryo,
  spark provided
)
