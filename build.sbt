name := "baldur"

version := "1.0"

scalaVersion := "2.10.4"

retrieveManaged := true

libraryDependencies ++= Seq("com.github.scopt" %% "scopt" % "3.3.0",
  "org.apache.spark" %% "spark-core" % "1.3.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.1")

resolvers += Resolver.sonatypeRepo("public")
