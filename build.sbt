name := "spark-util"
v"3.1.0"
+`2.11`
dep(
  paths % "1.5.0",
  hadoop provided,
  kryo,
  spark provided
)
