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
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.0",
  "com.typesafe.play" %% "play-json" % "2.3.4",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.3.0-M2",
  "uk.gov.hmrc" % "emailaddress_2.11" % "0.2.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4"
   )

resolvers ++= Seq(Resolver.sonatypeRepo("public"),
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.bintrayRepo("hmrc", "releases"))

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "kafka", xs @ _) => MergeStrategy.first
  case PathList("com", "datastax", "spark", "connector", xs @ _*) => MergeStrategy.first
  case PathList("org", "objenesis", xs @ _*) => MergeStrategy.first
  case PathList("com", "codehale", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.last
}
