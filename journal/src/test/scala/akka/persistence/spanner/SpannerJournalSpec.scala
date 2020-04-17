/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

object SpannerJournalSpec {
  val dbName = "SpannerJournalSpec"
  val config = ConfigFactory.parseString("""

      """).withFallback(SpannerSpec.config(dbName))
}

class SpannerJournalSpec extends JournalSpec(SpannerJournalSpec.config) with SpannerLifecycle {
  override def databaseName: String = SpannerJournalSpec.dbName
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()
}
