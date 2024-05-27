ThisBuild / version := "0.1.0-SNAPSHOT"

// NOTE:kafka-streams-scala does not support Scala3, use Scala2.13
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "KStreamDock",
    libraryDependencies ++= Seq(
      dependencies.typesafe,
      dependencies.logging,
      dependencies.logback,
      dependencies.kafka,
      dependencies.kafkaStream,
      "org.apache.kafka" %% "kafka-streams-scala" % "3.7.0"
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
    val logging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
    val logback = "ch.qos.logback" % "logback-classic" % "1.5.6"
  }
