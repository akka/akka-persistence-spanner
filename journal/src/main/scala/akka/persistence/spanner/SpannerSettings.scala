/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.annotation.InternalApi
import akka.persistence.spanner.SpannerSettings.SessionPoolSettings
import com.typesafe.config.Config
import scala.jdk.DurationConverters._

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
    val retryCreateInterval = config.getDuration("retry-create-interval").toScala
    val maxOutstandingRequests = config.getInt("max-outstanding-requests")
    val restartMinBackoff = config.getDuration("restart-min-backoff").toScala
    val restartMaxBackoff = config.getDuration("restart-max-backoff").toScala
    val keepAliveInterval = config.getDuration("keep-alive-interval").toScala
  }
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
  val table = config.getString("table")
  val deletionsTable = config.getString("deletions-table")
  val grpcClient = config.getString("grpc-client")

  val sessionPool = new SessionPoolSettings(config.getConfig("session-pool"))
}
