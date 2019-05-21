name := "griduHadoop"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.16"
libraryDependencies += "joda-time" % "joda-time" % "2.10.1"
libraryDependencies += "com.opencsv" % "opencsv" % "4.0"
libraryDependencies += "commons-io" % "commons-io" % "2.6"