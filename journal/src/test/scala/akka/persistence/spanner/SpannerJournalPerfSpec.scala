package akka.persistence.spanner

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

object SpannerJournalPerfSpec {
  val dbName = "SpannerJournalPerfSpec"

  val config = ConfigFactory
    .parseString(if (SpannerSpec.realSpanner) {
      // note that it needs a service account or else access is seriously throttled
      """
        akka.persistence.spanner.session-acquisition-timeout = 10s
        akka.persistence.spanner.session-pool.max-size = 10
        akka.persistence.spanner.session-pool.max-outstanding-requests = 10000
      """
    } else {
      // note that the spanner emulator is limited to a single transaction at a time
      // therefore the perf numbers using that aren't very useful, but it is good as an additional
      // stress test.
      """
        akka.persistence.spanner.session-pool.max-size = 1
        akka.persistence.spanner.session-pool.max-outstanding-requests = 1000
      """
    })
    .withFallback(SpannerSpec.config(dbName))
}

class SpannerJournalPerfSpec extends JournalPerfSpec(SpannerJournalPerfSpec.config) with SpannerLifecycle {
  override def databaseName: String = SpannerJournalPerfSpec.dbName

  override def shouldDumpRows: Boolean = false

  // tune event count down since emulator only does one transaction at a time and
  // bombarding real spanner does also not work great
  override def eventsCount: Int = 1000

  override def awaitDurationMillis: Long = 60.seconds.toMillis

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()
}
