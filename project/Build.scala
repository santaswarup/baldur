/**
 * Created by eric.meisel on 7/22/2015.
 */

import sbt._

object MyBuild extends Build {
	lazy val root = Project("root", file(".")) dependsOn(directKafkaStreamEnhancements)
	lazy val directKafkaStreamEnhancements = RootProject(uri("git://github.com/nrstott/DirectKafkaStreamEnhancements"))
}