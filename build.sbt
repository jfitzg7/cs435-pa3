name := "PA3"

version := "1.0"

scalaVersion := "2.12.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.1"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + module.revision + "." + artifact.extension
}