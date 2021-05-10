/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.persistence.CapabilityFlag
import akka.persistence.snapshot.SnapshotStoreSpec
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object SpannerSnapshotStoreSpec {
  val dbName = "SpannerSnapshotStoreSpec"
  val dbNameWithMeta = "SnapStoreSpecWithMeta"

  val config = ConfigFactory
    .parseString("""
    akka.loglevel=DEBUG
    akka.persistence.snapshot-store.plugin = "akka.persistence.spanner.snapshot"
    """)
    .withFallback(SpannerSpec.config(dbName))

  def configWithMeta =
    ConfigFactory
      .parseString("""
      akka.persistence.snapshot-store.plugin = "akka.persistence.spanner.snapshot"
      akka.persistence.spanner.with-meta = true
    """)
      .withFallback(SpannerSpec.config(dbNameWithMeta))
}

class SpannerSnapshotStoreSpec extends SnapshotStoreSpec(SpannerSnapshotStoreSpec.config) with SpannerLifecycle {
  override def customConfig = config
  override def databaseName: String = SpannerSnapshotStoreSpec.dbName

  override def withSnapshotStore: Boolean = true

  override def supportsSerialization: CapabilityFlag = true
}

class SpannerSnapshotStoreWithMetaSpec
    extends SnapshotStoreSpec(SpannerSnapshotStoreSpec.configWithMeta)
    with SpannerLifecycle {
  override def customConfig = config
  override def databaseName: String = SpannerSnapshotStoreSpec.dbNameWithMeta
  override def withSnapshotStore: Boolean = true
  protected override def supportsMetadata: CapabilityFlag = true
}
