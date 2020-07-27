/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

object SpannerJournalSpec {
  val dbName = "SpannerJournalSpec"
  val dbNameWithMeta = "SpannerJournalSpecWithMeta"
  val config = SpannerSpec.config(dbName)

  def configWithMeta =
    ConfigFactory
      .parseString("""akka.persistence.spanner.with-meta = true""")
      .withFallback(SpannerSpec.config(dbNameWithMeta))
}

class SpannerJournalSpec extends JournalSpec(SpannerJournalSpec.config) with SpannerLifecycle {
  override def customConfig = config
  override def databaseName: String = SpannerJournalSpec.dbName
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()
}

class SpannerJournalWithMetaSpec extends JournalSpec(SpannerJournalSpec.configWithMeta) with SpannerLifecycle {
  override def customConfig = config
  override def databaseName: String = SpannerJournalSpec.dbNameWithMeta
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()
  protected override def supportsMetadata: CapabilityFlag = CapabilityFlag.on()
}
