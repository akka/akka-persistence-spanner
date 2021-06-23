/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.annotation.{ApiMayChange, InternalStableApi}
import akka.persistence.query.Offset
import akka.persistence.spanner.internal.{SpannerGrpcClientExtension, SpannerObjectInteractions}
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.Future

/**
 * This API would eventually evolve into the akka-persistence object API
 *
 * INTERNAL API
 */
@InternalStableApi
class SpannerObjectStore(interactions: SpannerObjectInteractions) {
  import SpannerObjectStore._

  /**
   * @param seqNr sequence number for optimistic locking. starts at 1.
   */
  def upsertObject(
      entityType: String,
      persistenceId: PersistenceId,
      serId: Long,
      serManifest: String,
      value: ByteString,
      seqNr: Long
  ): Future[Unit] = interactions.upsertObject(entityType, persistenceId, serId, serManifest, value, seqNr)

  def getObject(persistenceId: PersistenceId): Future[Option[Result]] = interactions.getObject(persistenceId)

  def deleteObject(persistenceId: PersistenceId): Future[Unit] = interactions.deleteObject(persistenceId)

  /**
   * Get a source of the most recent changes made to objects of the given entity type since the passed in offset.
   *
   * Note that this only returns the most recent change to each object, if an object has been updated multiple times
   * since the offset, only the most recent of those changes will be part of the stream.
   *
   * This will return changes that occurred up to when the `Source` returned by this call is materialized. Changes to
   * objects made since materialization are not guaranteed to be included in the results.
   *
   * @param entityType The type of entity to get changes for.
   * @param offset The offset to get changes since. Must either be [[akka.persistence.query.NoOffset]] to get
   *               changes since the beginning of time, or an offset that has been previously returned by this query.
   *               Any other offsets are invalid.
   * @return A source of change events.
   */
  @ApiMayChange
  def currentChanges(entityType: String, offset: Offset): Source[Change, NotUsed] =
    interactions.currentChanges(entityType, offset)

  /**
   * Get a source of the most recent changes made to objects of the given entity type since the passed in offset.
   *
   * The returned source will never terminate, it effectively watches for changes to the objects and emits changes as
   * they happen.
   *
   * Not all changes that occur are guaranteed to be emitted, this call only guarantees that eventually, the most
   * recent change for each object since the offset will be emitted. In particular, multiple updates to a given object
   * in quick succession are likely to be skipped, with only the last update resulting in a change event from this
   * source.
   *
   * @param entityType The type of entity to get changes for.
   * @param offset The offset to get changes since. Must either be [[akka.persistence.query.NoOffset]] to get
   *               changes since the beginning of time, or an offset that has been previously returned by this query.
   *               Any other offsets are invalid.
   * @return A source of change events.
   */
  @ApiMayChange
  def changes(entityType: String, offset: Offset): Source[Change, NotUsed] =
    interactions.changes(entityType, offset)
}

/**
 * This API would eventually evolve into the akka-persistence object API
 *
 * INTERNAL API
 */
@InternalStableApi
object SpannerObjectStore {
  case class Result(byteString: ByteString, serId: Long, serManifest: String, seqNr: Long)
  case class Change(
      persistenceId: String,
      bytes: ByteString,
      serId: Long,
      serManifest: String,
      seqNr: Long,
      offset: Offset
  )

  def apply()(implicit system: ActorSystem): SpannerObjectStore = {
    val spannerSettings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))
    val grpcClient = SpannerGrpcClientExtension(system.toTyped).clientFor("akka.persistence.spanner")
    implicit val ec = system.dispatcher
    val spannerInteractions = new SpannerObjectInteractions(
      grpcClient,
      spannerSettings
    )
    new SpannerObjectStore(spannerInteractions)
  }
}
