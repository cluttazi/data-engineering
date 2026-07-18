// Gold-layer Spark job in Scala.
//
// Spark and Delta are `Provided`: the jar is executed by the pyspark-shipped
// spark-submit with `--packages io.delta:delta-spark_2.13:4.3.1`, so versions
// here MUST stay in lockstep with pyproject.toml (pyspark 4.1.1 / delta 4.3.1)
// — a mismatch surfaces as ClassNotFoundException at runtime, not at compile
// time. `Provided` deps remain on the test classpath, which is exactly what
// the ScalaTest suite needs.

ThisBuild / organization := "com.lakehouse"
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0"

val sparkVersion = "4.1.1"
val deltaVersion = "4.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "gold",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "io.delta" %% "delta-spark" % deltaVersion % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    // Spark on Java 17+/21 needs the JDK internals opened up; spark-submit
    // does this automatically, forked sbt tests must do it themselves.
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Djdk.reflect.useDirectMethodHandle=false"
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xfatal-warnings"
    )
  )
