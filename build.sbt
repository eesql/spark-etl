name := "dtvengeance"

version := "0.1.0"

scalaVersion := "2.11.11"

resolvers += "Pentaho Repository" at "https://public.nexus.pentaho.org/content/groups/omni"


sparkVersion := "2.2.0"

sparkComponents ++= Seq("sql", "mllib", "hive")

libraryDependencies += "mrpowers" % "spark-daria" % "2.2.0_0.13.0" % "test"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.10"


libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.13"

// HBase dependency
libraryDependencies ++= Seq(
  "org.apache.hbase"   %   "hbase-client"           % "1.2.6" ,
  "org.apache.hbase"   %   "hbase-common"           % "1.2.6" ,
  "org.apache.hbase"   %   "hbase-server"           % "1.2.6"
)

// Hadoop Client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.4"


fork in Test := true
//javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion.value + "_" + module.revision + "." + artifact.extension
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}