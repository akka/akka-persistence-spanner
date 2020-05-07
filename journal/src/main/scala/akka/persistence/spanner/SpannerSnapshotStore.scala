/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.spanner.internal.{SpannerGrpcClientExtension, SpannerSnapshotInteractions}
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

final class SpannerSnapshotStore(config: Config, cfgPath: String) extends SnapshotStore {
  private implicit val system: ActorSystem = context.system
  private implicit val ec: ExecutionContext = context.dispatcher

  private val sharedConfigPath = cfgPath.replaceAll("""\.snapshot$""", "")
  private val journalSettings = new SpannerSettings(context.system.settings.config.getConfig(sharedConfigPath))

  private val grpcClient = SpannerGrpcClientExtension(context.system.toTyped).clientFor(sharedConfigPath)

  private val spannerInteractions = new SpannerSnapshotInteractions(
    grpcClient,
    journalSettings
  )

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]] = {
    log.debug("loadAsync [{}], criteria [{}]", persistenceId, criteria)
    spannerInteractions.findSnapshot(persistenceId, criteria)
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    log.debug("saveAsync [{}], [{}]", metadata, snapshot.getClass)
    spannerInteractions.saveSnapshot(metadata, snapshot)
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    log.debug("deleteAsync [{}]", metadata)
    val criteria =
      if (metadata.timestamp == 0L)
        SnapshotSelectionCriteria(maxSequenceNr = metadata.sequenceNr, minSequenceNr = metadata.sequenceNr)
      else
        SnapshotSelectionCriteria(
          maxSequenceNr = metadata.sequenceNr,
          minSequenceNr = metadata.sequenceNr,
          maxTimestamp = metadata.timestamp,
          minTimestamp = metadata.timestamp
        )

    spannerInteractions.deleteSnapshots(metadata.persistenceId, criteria)
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    log.debug("deleteAsync [{}], [{}]", persistenceId, criteria)
    spannerInteractions.deleteSnapshots(persistenceId, criteria)
  }
}
