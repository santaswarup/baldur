name := "baldur"

version := "1.0"

scalaVersion := "2.11.6"

retrieveManaged := true

libraryDependencies ++= Seq("com.github.scopt" %% "scopt" % "3.3.0",
  "org.apache.kafka" %% "kafka" % "0.8.2.1" % "compile"
    exclude("org.jboss.netty", "netty")
    exclude("com.sun.jmx", "jmxri")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("javax.jms", "jms")
    exclude("javax.mail", "mail")
    exclude("jline", "jline"),
  "org.apache.spark" %% "spark-core" % "1.4.0",
  "org.apache.spark" %% "spark-streaming" % "1.4.0",
  "com.typesafe.play" %% "play-json" % "2.3.4"
   )

resolvers ++= Seq(Resolver.sonatypeRepo("public"),
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/")

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "kafka", xs @ _) => MergeStrategy.first
  case PathList("com", "datastax", "spark", "connector", xs @ _*) => MergeStrategy.first
  case PathList("org", "objenesis", xs @ _*) => MergeStrategy.first
  case PathList("com", "codehale", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.last
}
