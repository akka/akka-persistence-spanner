/*
 * Copyright (C) 2009-2021 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.spanner

import akka.persistence.spanner.internal.SpannerGrpcClientExtension
import akka.persistence.spanner.internal.SpannerObjectInteractions
import akka.persistence.typed.PersistenceId
import akka.util.ByteString

class SpannerClientSpec extends SpannerSpec {
  override def withObjectStore: Boolean = true

  "The spanner client" must {
    "insert and read large chunks of data" in {
      val grpcClient = SpannerGrpcClientExtension(testKit.system).clientFor("akka.persistence.spanner")
      val objectInteractions = new SpannerObjectInteractions(grpcClient, spannerSettings)

      val pid = PersistenceId("size", "1")
      objectInteractions
        .upsertObject(
          "entity-1",
          pid,
          666L,
          "nah",
          // 10mb, spanner request limit
          ByteString.fromArrayUnsafe(Array.tabulate[Byte](10485760)(_.toByte)),
          1L
        )
        .futureValue

      objectInteractions.getObject(pid).futureValue
    }
  }
}
