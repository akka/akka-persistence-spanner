/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.persistence.CapabilityFlag
import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.ConfigFactory

object SpannerSnapshotStoreSpec {
  val dbName = "SpannerSnapshotStoreSpec"
}

class SpannerSnapshotStoreSpec
    extends SnapshotStoreSpec(
      config = ConfigFactory.parseString("""
    akka.loglevel=DEBUG
    akka.persistence.snapshot-store.plugin = "akka.persistence.spanner.snapshot"
    """).withFallback(SpannerSpec.config(SpannerSnapshotStoreSpec.dbName))
    )
    with SpannerLifecycle {
  override def databaseName: String = SpannerSnapshotStoreSpec.dbName

  override def withSnapshotStore: Boolean = true

  override def supportsSerialization: CapabilityFlag = true
}
