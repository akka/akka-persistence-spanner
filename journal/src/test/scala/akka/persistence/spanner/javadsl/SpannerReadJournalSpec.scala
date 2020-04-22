package akka.persistence.spanner.javadsl

import akka.actor.ExtendedActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.spanner.SpannerSpec

class SpannerReadJournalSpec extends SpannerSpec {
  "SpannerReadJournal" should {
    "load" in {
      PersistenceQuery(system).getReadJournalFor(classOf[SpannerReadJournal], SpannerReadJournal.Identifier)
    }

    system.asInstanceOf[ExtendedActorSystem].registerOnTermination(() => println("Terminating"))
  }
}
