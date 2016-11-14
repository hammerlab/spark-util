name := "spark-util"

version := "1.1.0"

libraryDependencies <++= libraries { v => Seq(
  v('spark)
)}
