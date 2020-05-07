/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.util.concurrent.ConcurrentHashMap

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorSystem, Extension, ExtensionId}
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.persistence.spanner.SpannerSettings
import akka.util.ccompat.JavaConverters._
import com.google.auth.oauth2.GoogleCredentials
import com.google.spanner.v1.SpannerClient
import io.grpc.auth.MoreCallCredentials

import scala.concurrent.{ExecutionContext, Future}

private[spanner] object SpannerGrpcClientExtension extends ExtensionId[SpannerGrpcClientExtension] {
  override def createExtension(system: ActorSystem[_]): SpannerGrpcClientExtension =
    new SpannerGrpcClientExtension(system)
}

/**
 * Share client between parts of the plugin
 */
@InternalApi
private[spanner] class SpannerGrpcClientExtension(system: ActorSystem[_]) extends Extension {
  private val sessions = new ConcurrentHashMap[String, SpannerGrpcClient]
  private implicit val classic = system.toClassic
  private implicit val ec: ExecutionContext = system.executionContext

  CoordinatedShutdown(system.toClassic).addTask("service-requests-done", "shutdown-grpc-clients") { () =>
    Future.traverse(sessions.values().asScala)(_.shutdown()).map(_ => Done)
  }

  def clientFor(configLocation: String): SpannerGrpcClient =
    sessions.computeIfAbsent(
      configLocation,
      configLocation => {
        val settings = new SpannerSettings(system.settings.config.getConfig(configLocation))
        val grpcClient: SpannerClient =
          if (settings.useAuth) {
            SpannerClient(
              GrpcClientSettings
                .fromConfig(settings.grpcClient)
                .withCallCredentials(
                  MoreCallCredentials.from(
                    GoogleCredentials.getApplicationDefault.createScoped("https://www.googleapis.com/auth/spanner.data")
                  )
                )
            )
          } else {
            SpannerClient(GrpcClientSettings.fromConfig(settings.grpcClient))
          }
        new SpannerGrpcClient(configLocation, grpcClient, system, settings)
      }
    )
}
