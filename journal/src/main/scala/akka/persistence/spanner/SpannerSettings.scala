/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.annotation.InternalApi
import akka.persistence.spanner.SpannerSettings.SessionPoolSettings
import com.typesafe.config.Config
import akka.util.JavaDurationConverters._

import scala.concurrent.duration.FiniteDuration

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object SpannerSettings {
  final class SessionPoolSettings(config: Config) {
    val maxSize = config.getInt("max-size")
    // Spanner only supports 100 sessions per gRPC channel. We'd need multiple channels to support
    // more
    require(maxSize <= 100, "session-pool.max-size must be <= 100")
    val retryCreateInterval = config.getDuration("retry-create-interval").asScala
    val maxOutstandingRequests = config.getInt("max-outstanding-requests")
    val restartMinBackoff = config.getDuration("restart-min-backoff").asScala
    val restartMaxBackoff = config.getDuration("restart-max-backoff").asScala
    val keepAliveInterval = config.getDuration("keep-alive-interval").asScala

    val statsInternal: Option[FiniteDuration] = config.getString("stats-interval").toLowerCase match {
      case "off" => None
      case _ => Some(config.getDuration("stats-interval").asScala)
    }

    val statsLogger = config.getString("stats-logger")
    val shutdownTimeout = config.getDuration("shutdown-timeout").asScala
  }
}

private[spanner] final class QuerySettings(config: Config) {
  val refreshInterval: FiniteDuration = config.getDuration("refresh-interval").asScala
}

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] final class SpannerSettings(config: Config) {
  val project = config.getString("project")
  val instance = config.getString("instance")
  val database = config.getString("database")

  val fullyQualifiedProject = s"projects/$project"
  val parent = s"$fullyQualifiedProject/instances/$instance"
  val fullyQualifiedDatabase = s"$parent/databases/$database"
  val useAuth = config.getBoolean("use-auth")
  val journalTable = config.getString("journal.table")
  val eventTagTable = config.getString("journal.event-tag-table")
  val deletionsTable = config.getString("journal.deletions-table")
  val grpcClient = config.getString("grpc-client")
  val maxWriteRetries = config.getInt("max-write-retries")
  val maxWriteRetryTimeout = config.getDuration("max-write-retry-timeout").asScala

  val snapshotsTable = config.getString("snapshot.table")
  val sessionPool = new SessionPoolSettings(config.getConfig("session-pool"))
  val sessionAcquisitionTimeout = config.getDuration("session-acquisition-timeout").asScala

  val querySettings = new QuerySettings(config.getConfig("query"))
}
