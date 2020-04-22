package akka.persistence.spanner.internal

import java.util.concurrent.ConcurrentHashMap

import akka.actor.typed.{ActorSystem, Extension, ExtensionId}
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.persistence.spanner.SpannerSettings
import com.google.auth.oauth2.GoogleCredentials
import com.google.spanner.v1.SpannerClient
import io.grpc.auth.MoreCallCredentials
import akka.actor.typed.scaladsl.adapter._

import scala.concurrent.ExecutionContext

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
                    GoogleCredentials.getApplicationDefault
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
