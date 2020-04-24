package akka.persistence.spanner.javadsl

import akka.persistence.query.PersistenceQuery
import akka.persistence.spanner.SpannerSpec

class SpannerReadJournalSpec extends SpannerSpec {
  "SpannerReadJournal" should {
    "load" in {
      PersistenceQuery(testKit.system).getReadJournalFor(classOf[SpannerReadJournal], SpannerReadJournal.Identifier)
    }
  }
}
