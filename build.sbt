name := "Splash"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies  ++= Seq(
            // other dependencies here
            "org.scalanlp" %% "breeze" % "0.10",
            // native libraries are not included by default. add this if you want them (as of 0.7)
            // native libraries greatly improve performance, but increase jar sizes.
            "org.scalanlp" %% "breeze-natives" % "0.10"
)

resolvers ++= Seq(
            // other resolvers here
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)