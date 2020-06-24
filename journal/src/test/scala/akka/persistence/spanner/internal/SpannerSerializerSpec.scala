/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.spanner.internal

import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.spanner.SpannerOffset
import akka.serialization.SerializationExtension
import org.scalatest.wordspec.AnyWordSpecLike

class SpannerSerializerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  private val serializer = new SpannerSerializer(system.classicSystem.asInstanceOf[ExtendedActorSystem])
  private val commitTimestamp = SpannerUtils.unixTimestampMillisToSpanner(System.currentTimeMillis())

  "SpannerSerializer" must {
    Seq(
      "SpannerOffset-1" -> SpannerOffset(commitTimestamp, Map.empty),
      "SpannerOffset-2" -> SpannerOffset(commitTimestamp, Map("pid1" -> 5L)),
      "SpannerOffset-3" -> SpannerOffset(commitTimestamp, Map("pid1" -> 5L, "pid2" -> 3L, "pid3" -> 7L))).foreach {
      case (scenario, item) =>
        s"resolve serializer for $scenario" in {
          SerializationExtension(system).findSerializerFor(item).getClass should be(classOf[SpannerSerializer])
        }

        s"serialize and de-serialize $scenario" in {
          verifySerialization(item)
        }
    }
  }

  def verifySerialization(item: AnyRef): Unit =
    serializer.fromBinary(serializer.toBinary(item), serializer.manifest(item)) should be(item)
}
