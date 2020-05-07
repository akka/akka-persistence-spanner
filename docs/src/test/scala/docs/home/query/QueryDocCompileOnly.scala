package docs.home.query

import akka.actor.typed.ActorSystem

object QueryDocCompileOnly {
  val system: ActorSystem[_] = ???

  //#create
  import akka.persistence.query.PersistenceQuery
  import akka.persistence.spanner.scaladsl.SpannerReadJournal

  val queries = PersistenceQuery(system).readJournalFor[SpannerReadJournal](SpannerReadJournal.Identifier)
  //#create
}
