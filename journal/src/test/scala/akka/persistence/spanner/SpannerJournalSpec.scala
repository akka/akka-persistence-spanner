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
  // can't use because it requires support for both esb and aa at the same time, but we need to list columns up front
  // for queries so we can't do that
  // protected override def supportsMetadata: CapabilityFlag = CapabilityFlag.on()
}
