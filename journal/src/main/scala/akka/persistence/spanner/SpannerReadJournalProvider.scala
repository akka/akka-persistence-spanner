package akka.persistence.spanner

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config

final class SpannerReadJournalProvider(system: ExtendedActorSystem, config: Config, cfgLocation: String)
    extends ReadJournalProvider {
  override def scaladslReadJournal(): scaladsl.SpannerReadJournal =
    new scaladsl.SpannerReadJournal(system, config, cfgLocation)

  override def javadslReadJournal() = new javadsl.SpannerReadJournal(scaladslReadJournal())
}
