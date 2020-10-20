/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._

object Dependencies {
  val Scala212 = "2.12.11"
  val Scala213 = "2.13.1"

  val AkkaVersion = System.getProperty("override.akka.version", "2.6.6")
  val AkkaVersionInDocs = AkkaVersion.take(3)
  // for example
  val AkkaHttpVersion = "10.1.11"
  val AkkaManagementVersion = "1.0.6"

  val SpannerVersion = "1.52.0"
  // Keep in sync with Akka gRPC
  val GrpcVersion = "1.30.2"
  val GoogleAuthVersion = "0.21.1"

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion
    val akkaClusterTyped = "com.typesafe.akka" %% "akka-cluster-typed" % AkkaVersion
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion
    val akkaDiscovery = "com.typesafe.akka" %% "akka-discovery" % AkkaVersion
    val akkaSerializationJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion
    val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion
    val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion

    // used in the end to end example
    val akkaManagement = "com.lightbend.akka.management" %% "akka-management" % AkkaManagementVersion
    val akkaManagementClusterBootstrap = "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % AkkaManagementVersion
    val akkaManagementClusterHttp = "com.lightbend.akka.management" %% "akka-management-cluster-http" % AkkaManagementVersion
    val akkaDiscoveryKubernetesApi = "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % AkkaManagementVersion

    val spannerProtobuf = "com.google.api.grpc" % "proto-google-cloud-spanner-v1" % SpannerVersion % "protobuf-src" // Apache-2.0
    val spannerAdminProtobuf = "com.google.api.grpc" % "proto-google-cloud-spanner-admin-database-v1" % SpannerVersion % "protobuf-src" // Apache-2.0
    val spannerAdminInstanceProtobuf = "com.google.api.grpc" % "proto-google-cloud-spanner-admin-instance-v1" % SpannerVersion % "protobuf-src" // Apache-2.0

    val grpcAuth = "io.grpc" % "grpc-auth" % GrpcVersion // Apache-2.0
    val googleAuth = "com.google.auth" % "google-auth-library-oauth2-http" % GoogleAuthVersion // "BSD 3-Clause"

    val hdrHistogram = "org.hdrhistogram" % "HdrHistogram" % "2.1.12" // public domain / CC0 / BSD 2
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3" // EPL 1.0 / LGPL 2.1
  }

  object TestDeps {
    val akkaPersistenceTck = "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test
    val akkaTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
    val akkaJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion % Test

    val logback = Compile.logback % Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1" % Test // ApacheV2
    val junit = "junit" % "junit" % "4.12" % Test // Eclipse Public License 1.0
    val junitInterface = "com.novocode" % "junit-interface" % "0.11" % Test // "BSD 2-Clause"
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
    Compile.akkaActorTyped,
    Compile.akkaPersistenceTyped,
    Compile.akkaPersistenceQuery,
    Compile.akkaClusterTyped,
    Compile.akkaClusterShardingTyped,
    Compile.akkaSerializationJackson,
    Compile.akkaDiscovery,
    Compile.akkaSlf4j,
    Compile.logback,
    Compile.akkaManagement,
    Compile.akkaManagementClusterBootstrap,
    Compile.akkaManagementClusterHttp,
    Compile.akkaHttpSprayJson,
    Compile.hdrHistogram
  )

  val testkit = Seq(
    scalaTest,
    akkaTestkit,
    TestDeps.logback,
    TestDeps.junit,
    TestDeps.junitInterface
  )
}
