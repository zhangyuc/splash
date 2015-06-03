name := "Splash"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.0"

libraryDependencies  ++= Seq(
            "org.scalanlp" %% "breeze" % "0.11.1",
            "org.scalanlp" %% "breeze-natives" % "0.11.1"
)

resolvers ++= Seq(
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)