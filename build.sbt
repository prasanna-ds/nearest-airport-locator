import sbt.{Credentials, Path}

name := "search-nearby-airports"
organization := "com.travelaudience.data"
version := "1.0"
scalaVersion := "2.12.10"
credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
resolvers += Resolver.mavenLocal
inThisBuild(List(assemblyJarName in assembly := "search-nearby-airports.jar"))
libraryDependencies ++= {
  sys.props += "packaging.type" -> "jar"
  Seq(
    "org.scalatest"              %% "scalatest"     % "3.2.9",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
    "org.apache.spark"           %% "spark-sql"     % "3.1.2",
    "org.apache.spark"           %% "spark-core"    % "3.1.2",
  )
}

fork in run := true
assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*)                                       => MergeStrategy.discard
  case x                                                                   => MergeStrategy.first
}
