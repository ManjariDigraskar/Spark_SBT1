name := "Spark-example"

version := "0.1"

//scalaVersion := "2.11.8"
//scalaVersion := "2.11.12"
scalaVersion := "2.12.10"

autoScalaLibrary := false

//val sparkVersion = "2.3.4"
//val sparkVersion = "2.4.5"
val sparkVersion = "3.0.0-preview2"
val log4jVersion = "2.4.1"

val sparkDependencies = Seq (
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,

  "org.apache.spark" % "spark-streaming_2.12" %  sparkVersion % "provided"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies
