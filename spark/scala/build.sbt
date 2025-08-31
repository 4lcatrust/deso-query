ThisBuild / scalaVersion := "2.12.18"
val sparkV = "3.5.0"  // match your cluster

name := "scala-jobs"
version := "0.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkV % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkV % "provided"
)
