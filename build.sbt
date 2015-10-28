name := "Splash"

version := "0.2.0"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "-" + module.revision + "." + artifact.extension
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1"

libraryDependencies  ++= Seq(
            "org.scalanlp" %% "breeze" % "0.11.1",
            "org.scalanlp" %% "breeze-natives" % "0.11.1"
)

resolvers ++= Seq(
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)