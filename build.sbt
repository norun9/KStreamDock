ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "KStreamDock",
    libraryDependencies ++= Seq(
      dependencies.typesafe,
      dependencies.slf4j,
      dependencies.logback,
      dependencies.kafka,
      dependencies.kafkaStream
    )
  )

lazy val dependencies =
  new {
    val kafkaVersion = "3.7.0"
    val typesafeVersion = "1.4.3"
    val kafka = "org.apache.kafka" % "kafka-clients" % kafkaVersion
    val kafkaStream = "org.apache.kafka" % "kafka-streams" % kafkaVersion
//    val kafka = ("org.apache.kafka" %% "kafka-clients" :: "org.apache.kafka" %% "kafka-streams" :: Nil).map(_ % kafkaVersion)
    val typesafe = "com.typesafe" % "config" % typesafeVersion
    val slf4j = "org.slf4j" % "slf4j-jdk-platform-logging" % "2.0.12"
    val logback = "ch.qos.logback" % "logback-classic" % "1.5.6"
  }
