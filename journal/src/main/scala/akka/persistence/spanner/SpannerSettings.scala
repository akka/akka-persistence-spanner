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
    val retryCreateInterval = config.getDuration("retry-create-interval").toScala
    val maxOutstandingRequests = config.getInt("max-outstanding-requests")
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

  val sessionPool = new SessionPoolSettings(config.getConfig("session-pool"))
}
