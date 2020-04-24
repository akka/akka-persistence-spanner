package akka.persistence.spanner

import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.spanner.EventsByPersistenceIdQuerySpec.{Current, Live, QueryType}
import akka.persistence.spanner.TestActors.Persister.PersistMe
import akka.persistence.spanner.scaladsl.SpannerReadJournal
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.{Done, NotUsed}

object EventsByPersistenceIdQuerySpec {
  sealed trait QueryType
  case object Live extends QueryType
  case object Current extends QueryType
}

class EventsByPersistenceIdQuerySpec extends SpannerSpec {
  val query = PersistenceQuery(testKit.system)
    .readJournalFor[SpannerReadJournal](SpannerReadJournal.Identifier)

  List[QueryType](Live, Current) foreach { queryType =>
    def doQuery(pid: String, from: Long, to: Long): Source[EventEnvelope, NotUsed] =
      queryType match {
        case Live =>
          query.eventsByPersistenceId(pid, from, to)
        case Current =>
          query.currentEventsByPersistenceId(pid, from, to)
      }

    def assertFinished(probe: TestSubscriber.Probe[_], liveShouldFinish: Boolean = false): Unit =
      queryType match {
        case Live if !liveShouldFinish =>
          probe.expectNoMessage()
          probe.cancel()
        case _ =>
          probe.expectComplete()
      }

    s"$queryType EventsByPersistenceId" should {
      "return all events then compete" in {
        val pid = nextPid
        val persister = testKit.spawn(TestActors.Persister(pid))
        val probe = testKit.createTestProbe[Done]()
        val events = (1 to 20) map { i =>
          val payload = s"e-$i"
          persister ! PersistMe(payload, probe.ref)
          probe.expectMessage(Done)
          payload
        }

        val sub = doQuery(pid, 0, Long.MaxValue)
          .map(_.event)
          .runWith(TestSink.probe)

        sub
          .request(events.size + 1)
          .expectNextN(events)

        assertFinished(sub)
      }

      "only return sequence nrs requested" in {
        val pid = nextPid
        val persister = testKit.spawn(TestActors.Persister(pid))
        val probe = testKit.createTestProbe[Done]()
        val events = (1 to 20) map { i =>
          val payload = s"e-$i"
          persister ! PersistMe(payload, probe.ref)
          probe.expectMessage(Done)
          payload
        }

        val sub = doQuery(pid, 0, 5)
          .map(_.event)
          .runWith(TestSink.probe)

        sub
          .request(events.size + 1)
          .expectNextN(events.take(5))

        assertFinished(sub, liveShouldFinish = true)
      }
    }
  }

  "Live query" should {
    "pick up new events" in {
      val pid = nextPid
      val persister = testKit.spawn(TestActors.Persister(pid))
      val probe = testKit.createTestProbe[Done]()
      val sub = query
        .eventsByPersistenceId(pid, 0, Long.MaxValue)
        .map(_.event)
        .runWith(TestSink.probe)
      val events = (1 to 20) map { i =>
        val payload = s"e-$i"
        persister ! PersistMe(payload, probe.ref)
        probe.expectMessage(Done)
        payload
      }

      sub.request(21)
      sub.expectNextN(events)

      val events2 = (21 to 40) map { i =>
        val payload = s"e-$i"
        persister ! PersistMe(payload, probe.ref)
        probe.expectMessage(Done)
        payload
      }
      sub.request(21)
      sub.expectNextN(events2)

      sub.expectNoMessage()
      sub.cancel()
    }
  }
}
