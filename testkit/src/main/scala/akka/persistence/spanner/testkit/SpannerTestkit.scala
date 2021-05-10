/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner.testkit

import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.grpc.GrpcClientSettings
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.{SpannerJournalInteractions, SpannerSnapshotInteractions}
import com.google.auth.oauth2.GoogleCredentials
import com.google.spanner.admin.database.v1.{CreateDatabaseRequest, DatabaseAdminClient, DropDatabaseRequest}
import com.google.spanner.admin.instance.v1.{CreateInstanceRequest, InstanceAdminClient}
import com.typesafe.config.Config
import io.grpc.auth.MoreCallCredentials

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

private object SpannerTestkit {
  private var instanceCreated = false
  private def ensureInstanceCreated(
      client: InstanceAdminClient,
      spannerSettings: SpannerSettings,
      testkitSettings: SpannerTestkit.Settings
  )(implicit
      ec: ExecutionContext
  ): Unit =
    this.synchronized {
      if (!instanceCreated) {
        Await.ready(
          client
            .createInstance(CreateInstanceRequest(spannerSettings.fullyQualifiedProject, spannerSettings.instance))
            .recover {
              case t if t.getMessage.contains("ALREADY_EXISTS") =>
            },
          testkitSettings.operationTimeout
        )
        instanceCreated = true
      }
    }

  case class Settings(operationTimeout: FiniteDuration)
  object Settings {
    def apply(config: Config): Settings = {
      import akka.util.JavaDurationConverters._
      Settings(config.getDuration("operation-timeout").asScala)
    }
  }
}

/**
 * API may change
 */
@ApiMayChange
final class SpannerTestkit(systemProvider: ClassicActorSystemProvider) {
  private implicit val system = systemProvider.classicSystem
  private implicit val ec = system.dispatcher
  private val spannerSettings = new SpannerSettings(
    system.settings.config.getConfig("akka.persistence.spanner")
  )
  private val testkitSettings =
    SpannerTestkit.Settings(system.settings.config.getConfig("akka.persistence.spanner.testkit"))
  private val grpcSettings: GrpcClientSettings = if (spannerSettings.useAuth) {
    GrpcClientSettings
      .fromConfig(spannerSettings.grpcClient)
      .withCallCredentials(
        MoreCallCredentials.from(
          GoogleCredentials
            .getApplicationDefault()
            .createScoped(
              "https://www.googleapis.com/auth/spanner.admin",
              "https://www.googleapis.com/auth/spanner.data"
            )
        )
      )
  } else {
    GrpcClientSettings.fromConfig(spannerSettings.grpcClient)
  }

  // maybe create these once in an extension when we have lots of tests?
  private lazy val adminClient = DatabaseAdminClient(grpcSettings)
  private lazy val instanceClient = InstanceAdminClient(grpcSettings)

  /**
   * Attempts to creat the instance once per JVM and then creates the database.
   * Will fail if the database already exists.
   * Can be used per test if each test has its own database or before all tests
   * if the database is shared.
   */
  def createDatabaseAndSchema(): Unit = {
    SpannerTestkit.ensureInstanceCreated(instanceClient, spannerSettings, testkitSettings)
    Await.result(
      adminClient
        .createDatabase(
          CreateDatabaseRequest(
            parent = spannerSettings.parent,
            s"CREATE DATABASE ${spannerSettings.database}",
            SpannerJournalInteractions.Schema.Journal.journalTable(spannerSettings) ::
            SpannerJournalInteractions.Schema.Tags.tagTable(spannerSettings) ::
            SpannerJournalInteractions.Schema.Tags.eventsByTagIndex(spannerSettings) ::
            SpannerJournalInteractions.Schema.Deleted.deleteMetadataTable(spannerSettings) ::
            SpannerSnapshotInteractions.Schema.Snapshots.snapshotTable(spannerSettings) :: Nil
          )
        )
        .recover {
          case t if t.getMessage.contains("ALREADY_EXISTS") =>
        },
      testkitSettings.operationTimeout
    )
  }

  /**
   * Drops the configured database.
   */
  def dropDatabase(): Unit =
    Await.result(
      adminClient.dropDatabase(DropDatabaseRequest(spannerSettings.fullyQualifiedDatabase)),
      testkitSettings.operationTimeout
    )
}
