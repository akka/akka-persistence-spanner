/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.Done
import akka.persistence.query.PersistenceQuery
import akka.persistence.spanner.TestActors.Persister.PersistMe
import akka.persistence.spanner.scaladsl.SpannerReadJournal
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink

class PersistenceIdsQuerySpec extends SpannerSpec {
  val query = PersistenceQuery(testKit.system)
    .readJournalFor[SpannerReadJournal](SpannerReadJournal.Identifier)

  val pid1 = nextPid
  val pid2 = nextPid
  val probe = testKit.createTestProbe[Done]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    testKit.spawn(TestActors.Persister(pid1)) ! PersistMe("cat", probe.ref)
    probe.expectMessage(Done)
    testKit.spawn(TestActors.Persister(pid2)) ! PersistMe("dog", probe.ref)
    probe.expectMessage(Done)
  }

  "currentPersistenceIds" must {
    "work" in {
      val pids = query.currentPersistenceIds().runWith(Sink.seq).futureValue
      pids.size shouldEqual 2
      pids.toSet shouldEqual Set(pid1, pid2)
    }
  }

  "live persistenceIds" must {
    "show new persistence ids" in {
      val probe = testKit.createTestProbe[Done]()

      val pids = query.persistenceIds().runWith(TestSink.probe)
      pids.request(10)
      pids.expectNextN(2).toSet shouldEqual Set(pid1, pid2) // the two from setup
      pids.expectNoMessage()

      val pid3 = nextPid
      val pid4 = nextPid
      testKit.spawn(TestActors.Persister(pid3)) ! PersistMe("cat", probe.ref)
      testKit.spawn(TestActors.Persister(pid4)) ! PersistMe("dog", probe.ref)

      // should not get pid1 and pid2 again
      pids.expectNextN(2).toSet shouldEqual Set(pid3, pid4)
      pids.expectNoMessage()
      pids.cancel()
    }
  }
}
