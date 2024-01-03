name := "Benchspark"
version := "1.0"

// Choose a suitable scala version
scalaVersion := "2.12.14"

// Choose a suitable spark version
val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % sparkVersion % "provided"),
  ("org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"),
  ("org.apache.spark" %% "spark-graphx" % sparkVersion % "provided")
)

