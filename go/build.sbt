name := "simple-spark-scala"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.1",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.1.0.jre7"
)