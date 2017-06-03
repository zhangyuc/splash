name := "splash"

version := "0.2.0"

scalaVersion := "2.10.6"
crossScalaVersions := Seq("2.10.6", "2.11.11")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "-" + module.revision + "." + artifact.extension
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" // % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.3" // % "provided"

libraryDependencies  ++= Seq(
            "com.nativelibs4java" %% "scalaxy-loops" % "[0.3.4,)",
            "org.scalanlp" %% "breeze" % "[0.11.2,)",
            "org.scalanlp" %% "breeze-natives" % "[0.11.2,)"
)

resolvers ++= Seq(
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)
