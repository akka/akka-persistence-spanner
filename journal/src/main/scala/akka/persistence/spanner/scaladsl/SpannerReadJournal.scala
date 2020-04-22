/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.scaladsl

import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.spanner.internal.SpannerGrpcClientExtension
import com.typesafe.config.Config

object SpannerReadJournal {
  val Identifier = "akka.persistence.spanner.query"
}

final class SpannerReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String) extends ReadJournal {
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")

  private val grpcClient = SpannerGrpcClientExtension(system.toTyped).clientFor(sharedConfigPath)
}
