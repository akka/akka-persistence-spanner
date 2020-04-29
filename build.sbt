import sbt.Keys.parallelExecution

inThisBuild(
  Seq(
    scalaVersion := "2.13.1",
    organization := "com.lightbend.akka",
    organizationName := "Lightbend Inc.",
    homepage := Some(url("https://doc.akka.io/docs/akka-persistence-spanner/current")),
    scmInfo := Some(
        ScmInfo(
          url("https://github.com/akka/akka-persistence-spanner"),
          "https://github.com/akka/akka-persistence-spanner.git"
        )
      ),
    startYear := Some(2020),
    developers += Developer(
        "contributors",
        "Contributors",
        "https://gitter.im/akka/dev",
        url("https://github.com/akka/akka-persistence-spanner/graphs/contributors")
      ),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    description := "A replicated Akka Persistence journal backed by Spanner",
    // due to the emulator
    parallelExecution := false
  )
)

def common: Seq[Setting[_]] = Seq(
  crossScalaVersions := Seq(Dependencies.Scala213, Dependencies.Scala212),
  scalaVersion := Dependencies.Scala212,
  crossVersion := CrossVersion.binary,
  scalafmtOnCompile := true,
  bintrayOrganization := Some("akka"),
  bintrayPackage := "akka-persistence-spanner",
  bintrayRepository := (if (isSnapshot.value) "snapshots" else "maven"),
  // Setting javac options in common allows IntelliJ IDEA to import them automatically
  javacOptions in compile ++= Seq(
      "-encoding",
      "UTF-8",
      "-source",
      "1.8",
      "-target",
      "1.8"
    ),
  headerLicense := Some(
      HeaderLicense.Custom(
        """Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>"""
      )
    ),
  logBuffered in Test := System.getProperty("akka.logBufferedTests", "false").toBoolean,
  // show full stack traces and test case durations
  testOptions in Test += Tests.Argument("-oDF"),
  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -a Show stack traces and exception class name for AssertionErrors.
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
  projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value)
)

lazy val dontPublish = Seq(
  skip in publish := true,
  whitesourceIgnore := true,
  publishArtifact in Compile := false
)

lazy val root = (project in file("."))
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "akka-persistence-spanner-root",
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
  )
  .aggregate(journal)

lazy val dumpSchema = taskKey[Unit]("Dumps schema for docs")
dumpSchema := (journal / runMain in (Test)).toTask(" akka.persistence.spanner.PrintSchema").value

lazy val journal = (project in file("journal"))
  .enablePlugins(AkkaGrpcPlugin)
  .settings(common)
  .settings(
    name := "akka-persistence-spanner",
    libraryDependencies ++= Dependencies.journal
  )

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PublishRsyncPlugin)
  .dependsOn(journal)
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "Akka Persistence Spanner",
    (Compile / paradox) := (Compile / paradox).dependsOn(root / dumpSchema).value,
    crossScalaVersions := Seq(Dependencies.Scala212),
    previewPath := (Paradox / siteSubdirName).value,
    Paradox / siteSubdirName := s"docs/akka-persistence-spanner/${projectInfoVersion.value}",
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxProperties ++= Map(
        "akka.version" -> Dependencies.AkkaVersion,
        "extref.akka-docs.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersion}/%s",
        "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
        "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersion}",
        "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/"
      ),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifact := makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io"
  )
