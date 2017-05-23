name := "billhistCDT"
version := "1.0"
scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core" % "1.6.0",
  "org.apache.spark" % "spark-sql" % "1.6.0",
  "org.apache.spark" % "spark-hive" % "1.6.0" ,
  "org.apache.spark" %% "spark-streaming" % "1.6.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0",
  "org.apache.spark" %% "spark-streaming-flume" % "1.6.0",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.1.0.jre7",
    "org.postgresql" % "postgresql" % "9.4-1200-jdbc41"
)