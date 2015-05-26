name := "baldur"

version := "1.0"

scalaVersion := "2.10.4"

retrieveManaged := true

libraryDependencies ++= Seq("com.github.scopt" %% "scopt" % "3.3.0",
  "org.apache.kafka" %% "kafka" % "0.8.2.1" % "compile"
    exclude("org.jboss.netty", "netty")
    exclude("com.sun.jmx", "jmxri")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("javax.jms", "jms")
    exclude("javax.mail", "mail")
    exclude("jline", "jline"),
  "org.apache.spark" %% "spark-core" % "1.3.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.1")

resolvers += Resolver.sonatypeRepo("public")
