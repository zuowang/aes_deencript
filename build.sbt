name := "AES"
version := "0.0.1"
organization := "cn.wanda.idc"
scalaVersion := "2.10.4"
scalacOptions ++= Seq("-unchecked", "-deprecation")
resolvers += "CDH5" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/ivy-releases/"
resolvers ++= Seq( "maven.org" at "http://repo2.maven.org/maven2",
                   "conjars.org" at "http://conjars.org/repo",
                   "codahale.com" at "http://repo.codahale.com" )

libraryDependencies += "org.apache.hadoop" %  "hadoop-core"        % "1.2.1"      % "provided"
libraryDependencies += "org.apache.hive"   %  "hive-exec"          % "1.1.0"       % "provided"
