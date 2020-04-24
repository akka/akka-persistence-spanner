package akka.persistence.spanner

import akka.{Done, NotUsed}
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, PersistenceQuery}
import akka.persistence.spanner.EventsByTagSpec.{Current, Live, QueryType}
import akka.persistence.spanner.TestActors.Tagger.WithTags
import akka.persistence.spanner.scaladsl.SpannerReadJournal
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import scala.concurrent.duration._

object EventsByTagSpec {
  sealed trait QueryType
  case object Live extends QueryType
  case object Current extends QueryType
}

class EventsByTagSpec extends SpannerSpec {
  val query = PersistenceQuery(testKit.system)
    .readJournalFor[SpannerReadJournal](SpannerReadJournal.Identifier)

  class Setup {
    val persistenceId = nextPid
    val tagger = testKit.spawn(TestActors.Tagger(persistenceId))
    val probe = testKit.createTestProbe[Done]
    val tag = nextTag
    val tags = Set(tag)
    val sinkProbe = TestSink.probe[EventEnvelope]
  }

  List[QueryType](Current) foreach { queryType =>
    def doQuery(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
      queryType match {
        case Live =>
          query.eventsByTag(tag, offset)
        case Current =>
          query.currentEventsByTag(tag, offset)
      }

    def assertFinished(probe: TestSubscriber.Probe[EventEnvelope]): Unit =
      queryType match {
        case Live =>
          probe.expectNoMessage()
          probe.cancel()
        case Current =>
          probe.expectComplete()
      }

    s"$queryType EventsByTag" should {
      "return all events for NoOffset" in new Setup {
        for (i <- 1 to 20) {
          tagger ! WithTags(s"e-$i", tags, probe.ref)
          probe.expectMessage(Done)
        }
        val result: TestSubscriber.Probe[EventEnvelope] = doQuery(tag, NoOffset)
          .runWith(sinkProbe)
          .request(21)
        for (i <- 1 to 20) {
          val expectedEvent = s"e-$i"
          withClue(s"Expected event $expectedEvent") {
            result.expectNextPF {
              case EventEnvelope(_, _, _, WithTags(`expectedEvent`, `tags`, _)) =>
            }
          }
        }
        assertFinished(result)
      }

      "only return events after an offset" in new Setup {
        for (i <- 1 to 20) {
          tagger ! WithTags(s"e-$i", tags, probe.ref)
          probe.expectMessage(Done)
        }

        val result: TestSubscriber.Probe[EventEnvelope] = doQuery(tag, NoOffset)
          .runWith(sinkProbe)
          .request(21)

        result.expectNextN(9)

        val offset = result.expectNext().offset
        result.cancel()

        val withOffset = doQuery(tag, offset).runWith(TestSink.probe[EventEnvelope])
        withOffset.request(12)
        for (i <- 11 to 20) {
          val expectedEvent = s"e-$i"
          withClue(s"Expected event $expectedEvent") {
            withOffset.expectNextPF {
              case EventEnvelope(
                  SpannerOffset(_, seen),
                  persistenceId,
                  sequenceNr,
                  WithTags(`expectedEvent`, `tags`, _)
                  ) if seen(persistenceId) == sequenceNr =>
            }
          }
        }
        assertFinished(withOffset)
      }

      "filter events with the same timestamp based on seen sequence nrs" in new Setup {
        tagger ! WithTags(s"e-1", tags, probe.ref)
        probe.expectMessage(Done)
        val singleEvent: EventEnvelope = doQuery(tag, NoOffset).runWith(Sink.head).futureValue
        val offset = singleEvent.offset.asInstanceOf[SpannerOffset]
        offset.seen shouldEqual Map(singleEvent.persistenceId -> singleEvent.sequenceNr)

        doQuery(tag, offset).runWith(Sink.headOption).futureValue shouldEqual None
      }

      "not filter events with the same timestamp based on sequence nrs" in new Setup {
        tagger ! WithTags(s"e-1", tags, probe.ref)
        probe.expectMessage(Done)
        val singleEvent: EventEnvelope = doQuery(tag, NoOffset).runWith(Sink.head).futureValue
        val offset = singleEvent.offset.asInstanceOf[SpannerOffset]
        offset.seen shouldEqual Map(singleEvent.persistenceId -> singleEvent.sequenceNr)

        val offsetWithoutSeen = SpannerOffset(offset.commitTimestamp, Map.empty)
        doQuery(tag, offsetWithoutSeen).runWith(Sink.headOption).futureValue shouldEqual Some(singleEvent)
      }
    }
  }

  // tests just relevant for live query
  "Live events by tag" should {
    "empty query returns session" in {
      val result = query
        .eventsByTag("no-events", NoOffset)
        .runWith(TestSink.probe)
        .request(10)

      // should keep querying and being able to get sessions without failing
      result.expectNoMessage(6.seconds)

      result.cancel()
    }
    "find new events" in new Setup {
      for (i <- 1 to 20) {
        tagger ! WithTags(s"e-$i", tags, probe.ref)
        probe.expectMessage(Done)
      }
      val result: TestSubscriber.Probe[EventEnvelope] = query
        .eventsByTag(tag, NoOffset)
        .runWith(sinkProbe)
        .request(21)
      for (i <- 1 to 20) {
        val expectedEvent = s"e-$i"
        withClue(s"Expected event $expectedEvent") {
          result.expectNextPF {
            case EventEnvelope(_, _, _, WithTags(`expectedEvent`, `tags`, _)) =>
          }
        }
      }

      for (i <- 21 to 40) {
        tagger ! WithTags(s"e-$i", tags, probe.ref)
        probe.expectMessage(10.seconds, Done)
      }

      result.request(21)

      for (i <- 21 to 40) {
        val expectedEvent = s"e-$i"
        withClue(s"Expected event $expectedEvent") {
          result.expectNextPF {
            case EventEnvelope(_, _, _, WithTags(`expectedEvent`, `tags`, _)) =>
          }
        }
      }
    }
  }
}
