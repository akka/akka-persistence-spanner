/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.persistence.query.NoOffset
import akka.persistence.spanner.SpannerObjectStore.{Change, Result}
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString
import scala.concurrent.duration._

class SpannerObjectStoreSpec extends SpannerSpec("SpannerObjectStoreSpec") {
  override def withObjectStore: Boolean = true

  val spannerInteractions = SpannerObjectStore()

  // All objects in this test are carts
  val entityType = "cart"
  // All objects in this test use the same serialization ;)
  val serId = 5749231L
  val serManifest = "manifest-type-information"

  "The spanner object store" should {
    "save and retrieve a value" in {
      val persistenceId = PersistenceId(entityType, "my-persistenceId")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, seqNr = 1L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 1L)))
    }
    "save and retrieve a binary value" in {
      val persistenceId = PersistenceId(entityType, "my-id-for-binary-value")
      // this is not a valid UTF-8 string:
      val value = ByteString(Array[Byte](0xc0.toByte, 0xc1.toByte))
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, seqNr = 1L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 1L)))
    }
    "produce None when fetching a non-existing key" in {
      val key = PersistenceId(entityType, "nonexistent-id")
      spannerInteractions.getObject(key).futureValue should be(None)
    }
    "update a value" in {
      val persistenceId = PersistenceId(entityType, "id-to-be-updated")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, seqNr = 1L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 1L)))

      val updatedValue = ByteString("Open to Feedback")
      spannerInteractions
        .upsertObject(entityType, persistenceId, serId, serManifest, updatedValue, seqNr = 2L)
        .futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(
        Some(Result(updatedValue, serId, serManifest, 2L))
      )
    }
    "detect and reject concurrent inserts" in {
      val persistenceId = PersistenceId(entityType, "id-to-be-inserted-concurrently")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, seqNr = 1L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 1L)))

      val updatedValue = ByteString("Open to Feedback")
      val failure =
        spannerInteractions
          .upsertObject(entityType, persistenceId, serId, serManifest, updatedValue, seqNr = 1L)
          .failed
          .futureValue
      failure.getMessage should include(s"Insert failed: object for persistence id [$persistenceId] already exists")
    }
    "detect and reject concurrent updates" in {
      val persistenceId = PersistenceId(entityType, "id-to-be-updated-concurrently")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, seqNr = 1L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 1L)))

      val updatedValue = ByteString("Open to Feedback")
      spannerInteractions
        .upsertObject(entityType, persistenceId, serId, serManifest, updatedValue, seqNr = 2L)
        .futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(
        Some(Result(updatedValue, serId, serManifest, 2L))
      )

      // simulate an update by a different node that didn't see the first one:
      val updatedValue2 = ByteString("Genuine and Sincere in all Communications")
      spannerInteractions
        .upsertObject(entityType, persistenceId, serId, serManifest, updatedValue2, 2L)
        .failed
        .futureValue
    }
    "support deletions" in {
      val persistenceId = PersistenceId(entityType, "to-be-added-and-removed")
      val value = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId, serId, serManifest, value, seqNr = 1L).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(Some(Result(value, serId, serManifest, 1L)))
      spannerInteractions.deleteObject(persistenceId).futureValue
      spannerInteractions.getObject(persistenceId).futureValue should be(None)
    }
    "support querying for current changes" in {
      val entityType = "current-changes"
      val persistenceId1 = PersistenceId(entityType, "id-1")
      val value1 = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId1, serId, serManifest, value1, seqNr = 1L).futureValue
      val persistenceId2 = PersistenceId(entityType, "id-2")
      val value2 = ByteString("Open to Feedback")
      spannerInteractions.upsertObject(entityType, persistenceId2, serId, serManifest, value2, seqNr = 1L).futureValue

      val changes1 = spannerInteractions.currentChanges(entityType, NoOffset).runWith(Sink.seq).futureValue
      changes1 should have size 2
      val change1 = changes1.head
      change1 should be(Change(persistenceId1.id, value1, serId, serManifest, 1L, change1.offset))
      val change2 = changes1(1)
      val expectedChange2 = Change(persistenceId2.id, value2, serId, serManifest, 1L, change2.offset)
      change2 should be(expectedChange2)

      val changes2 = spannerInteractions.currentChanges(entityType, change1.offset).runWith(Sink.seq).futureValue
      changes2 should have size 1
      changes2.head should be(expectedChange2)

      val value3 = ByteString("Genuine and Sincere in all Communications")
      spannerInteractions.upsertObject(entityType, persistenceId1, serId, serManifest, value3, seqNr = 2L).futureValue

      val changes3 = spannerInteractions.currentChanges(entityType, change2.offset).runWith(Sink.seq).futureValue
      changes3 should have size 1
      changes3.head should be(Change(persistenceId1.id, value3, serId, serManifest, 2L, changes3.head.offset))
    }
    "support continuous changes query" in {
      val entityType = "continuous-changes"
      val persistenceId1 = PersistenceId(entityType, "id-1")
      val value1 = ByteString("Genuinely Collaborative")
      spannerInteractions.upsertObject(entityType, persistenceId1, serId, serManifest, value1, seqNr = 1L).futureValue
      val persistenceId2 = PersistenceId(entityType, "id-2")
      val value2 = ByteString("Open to Feedback")
      spannerInteractions.upsertObject(entityType, persistenceId2, serId, serManifest, value2, seqNr = 1L).futureValue

      val probe = spannerInteractions.changes(entityType, NoOffset).runWith(TestSink.probe[Change])
      probe.request(100)

      val change1 = probe.expectNext()
      change1 should be(Change(persistenceId1.id, value1, serId, serManifest, 1L, change1.offset))
      val change2 = probe.expectNext()
      change2 should be(Change(persistenceId2.id, value2, serId, serManifest, 1L, change2.offset))
      probe.expectNoMessage(1.second)

      val persistenceId3 = PersistenceId(entityType, "id-3")
      val value3 = ByteString("Genuine and Sincere in all Communications")
      spannerInteractions.upsertObject(entityType, persistenceId3, serId, serManifest, value3, seqNr = 1L)
      val change3 = probe.expectNext()
      change3 should be(Change(persistenceId3.id, value3, serId, serManifest, 1L, change3.offset))

      val value4 = ByteString("Always prefer Hacks over Well Engineered Solutions")
      spannerInteractions.upsertObject(entityType, persistenceId1, serId, serManifest, value4, seqNr = 2L)
      val change4 = probe.expectNext()
      change4 should be(Change(persistenceId1.id, value4, serId, serManifest, 2L, change4.offset))

      probe.cancel()
    }
  }
}
