name := "lineage-datalake-spark-scala"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.0",
   "org.apache.spark" % "spark-sql_2.10" % "1.6.0")