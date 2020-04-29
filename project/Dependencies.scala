/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._

object Dependencies {
  val Scala212 = "2.12.11"
  val Scala213 = "2.13.1"

  val AkkaVersion = System.getProperty("override.akka.version", "2.6.4")
  // for example
  val AkkaManagementVersion = "1.0.6"

  val SpannerVersion = "1.52.0"
  // Keep in sync with Akka gRPC
  val GrpcVersion = "1.28.0"
  val GoogleAuthVersion = "0.20.0"

  object Compile {
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion
    val akkaDiscovery = "com.typesafe.akka" %% "akka-discovery" % AkkaVersion

    val spannerProtobuf = "com.google.api.grpc" % "proto-google-cloud-spanner-v1" % SpannerVersion % "protobuf-src"
    val spannerAdminProtobuf = "com.google.api.grpc" % "proto-google-cloud-spanner-admin-database-v1" % SpannerVersion % "protobuf-src"
    val spannerAdminInstanceProtobuf = "com.google.api.grpc" % "proto-google-cloud-spanner-admin-instance-v1" % SpannerVersion % "protobuf-src"

    val grpcAuth = "io.grpc" % "grpc-auth" % GrpcVersion
    val googleAuth = "com.google.auth" % "google-auth-library-oauth2-http" % GoogleAuthVersion

    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3" // EPL 1.0 / LGPL 2.1
  }

  object TestDeps {
    val akkaPersistenceTck = "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test
    val akkaTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
    val akkaJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion % Test

    val logback = Compile.logback % Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1" % Test // ApacheV2
    val junit = "junit" % "junit" % "4.12" % Test
    val junitInterface = "com.novocode" % "junit-interface" % "0.11" % Test
  }

  import Compile._
  import TestDeps._

  val journal = Seq(
    akkaPersistence,
    akkaPersistenceQuery,
    akkaPersistenceTck,
    akkaDiscovery,
    akkaStreamTestkit,
    spannerProtobuf,
    spannerAdminProtobuf,
    spannerAdminInstanceProtobuf,
    grpcAuth,
    googleAuth,
    akkaTestkit,
    akkaJackson,
    TestDeps.logback,
    scalaTest
  )

  val example = Seq(
    "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion,
    "com.typesafe.akka" %% "akka-discovery" % AkkaVersion,
    "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion,
    "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
    Compile.logback,
    "com.lightbend.akka.management" %% "akka-management" % AkkaManagementVersion,
    "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % AkkaManagementVersion,
    "com.lightbend.akka.management" %% "akka-management-cluster-http" % AkkaManagementVersion,
    "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % AkkaManagementVersion,
    "org.hdrhistogram" % "HdrHistogram" % "2.1.12"
  )
}
