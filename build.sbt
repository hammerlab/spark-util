name := "spark-util"

version := "1.0.1-SNAPSHOT"

libraryDependencies <++= libraries { v => Seq(
  v('spark)
)}
