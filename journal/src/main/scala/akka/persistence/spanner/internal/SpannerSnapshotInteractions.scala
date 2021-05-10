/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.util.Base64
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SpannerUtils.{spannerTimestampToUnixMillis, unixTimestampMillisToSpanner}
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.serialization.{Serialization, SerializationExtension, Serializers}
import com.google.protobuf.struct.Value.Kind
import com.google.protobuf.struct.Value.Kind.StringValue
import com.google.protobuf.struct.{ListValue, Struct, Value}
import com.google.spanner.v1.{Mutation, Type, TypeCode}
import io.grpc.{Status, StatusRuntimeException}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object SpannerSnapshotInteractions {
  object Schema {
    object Snapshots {
      private def metaColumns =
        """
          |   meta BYTES(MAX),
          |   meta_ser_id INT64,
          |   meta_ser_manifest STRING(MAX),
          |""".stripMargin

      def snapshotTable(settings: SpannerSettings): String =
        s"""CREATE TABLE ${settings.snapshotsTable} (
           |  persistence_id STRING(MAX) NOT NULL,
           |  sequence_nr INT64 NOT NULL,
           |  timestamp TIMESTAMP NOT NULL,
           |  ser_id INT64 NOT NULL,
           |  ser_manifest STRING(MAX) NOT NULL,
           |  snapshot BYTES(MAX),
           |  meta BYTES(MAX),
           |  meta_ser_id INT64,
           |  meta_ser_manifest STRING(MAX),
           |) PRIMARY KEY (persistence_id, sequence_nr)""".stripMargin

      val PersistenceId = "persistence_id" -> Type(TypeCode.STRING)
      val SeqNr = "sequence_nr" -> Type(TypeCode.INT64)
      val WriteTime = "timestamp" -> Type(TypeCode.TIMESTAMP)

      val SerId = "ser_id" -> Type(TypeCode.INT64)
      val SerManifest = "ser_manifest" -> Type(TypeCode.STRING)
      val Snapshot = "snapshot" -> Type(TypeCode.BYTES)

      val Meta = "meta" -> Type(TypeCode.BYTES)
      val MetaSerId = "meta_ser_id" -> Type(TypeCode.INT64)
      val MetaSerManifest = "meta_ser_manifest" -> Type(TypeCode.STRING)

      val Columns =
        Seq(PersistenceId, SeqNr, WriteTime, SerId, SerManifest, Snapshot, Meta, MetaSerId, MetaSerManifest)
          .map(_._1)
          .toList
    }
  }
}

/**
 * INTERNAL API
 *
 * Class for doing spanner interaction outside of an actor to avoid mistakes
 * in future callbacks
 */
@InternalApi
private[spanner] final class SpannerSnapshotInteractions(
    spannerGrpcClient: SpannerGrpcClient,
    settings: SpannerSettings
)(implicit
    ec: ExecutionContext,
    system: ActorSystem
) {
  import SpannerSnapshotInteractions.Schema.Snapshots
  import SpannerUtils.nullValue

  private val log = LoggerFactory.getLogger(getClass)

  private val serialization: Serialization = SerializationExtension(system)

  def findSnapshot(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val query =
      "SELECT persistence_id, sequence_nr, timestamp, ser_id, ser_manifest, snapshot, meta, meta_ser_id, meta_ser_manifest " +
      s"FROM ${settings.snapshotsTable} " +
      wherePartFor(criteria) +
      "ORDER BY sequence_nr DESC LIMIT 1"

    val queryParams = queryParamsFor(persistenceId, criteria)
    val queryParamTypes = queryParamTypesFor(criteria)

    spannerGrpcClient.withSession { implicit session =>
      if (log.isTraceEnabled()) {
        log.traceN(
          "findSnapshot: pid: {}, session: {}, criteria: {}, query: {}, params: {}, paramTypes: {}",
          persistenceId,
          session.id,
          criteria,
          query,
          queryParams,
          queryParamTypes
        )
      }

      // FIXME this executeQuery limits snapshot size to 10Mb, might not be enough.
      // streaming turned out to potentially not handle empty query result though
      spannerGrpcClient
        .executeQuery(
          query,
          queryParams,
          queryParamTypes
        )
        .map { result =>
          if (result.rows.isEmpty) None
          else if (result.rows.size > 1)
            throw new IllegalArgumentException(s"Expected a single row from db, got ${result.rows.size}")
          else {
            val fieldIterator = result.rows.head.values.iterator
            val persistenceId = fieldIterator.next().kind.stringValue.get
            val sequenceNr = fieldIterator.next().kind.stringValue.get.toLong
            val timestamp = spannerTimestampToUnixMillis(fieldIterator.next().kind.stringValue.get)
            val serId = fieldIterator.next().kind.stringValue.get.toInt // ints and longs are StringValues :|
            val serManifest = fieldIterator.next().kind.stringValue.get
            val snapshotBytes = Base64.getDecoder.decode(fieldIterator.next().kind.stringValue.get)

            val snapshot = serialization.deserialize(snapshotBytes, serId.toInt, serManifest).get

            val metadata = SnapshotMetadata(persistenceId, sequenceNr, timestamp)
            val firstMetaField = fieldIterator.next()
            Some(
              if (firstMetaField.kind.isNullValue) SelectedSnapshot(metadata, snapshot)
              else {
                val snapshotByteString = firstMetaField.getStringValue
                val metaSerId = fieldIterator.next().kind.stringValue.get.toInt // ints and longs are StringValues :|
                val metaSerManifest = fieldIterator.next().kind.stringValue.get
                val snapshotBytes = Base64.getDecoder.decode(snapshotByteString)
                val replicationMeta = serialization.deserialize(snapshotBytes, metaSerId, metaSerManifest).get
                SelectedSnapshot(metadata.withMetadata(replicationMeta), snapshot)
              }
            )
          }
        }
    }
  }

  def saveSnapshot(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
      val s2 = snapshot.asInstanceOf[AnyRef]
      val serializer = serialization.findSerializerFor(s2)
      val manifest = Serializers.manifestFor(serializer, s2)
      val snapshotBytes = Base64.getEncoder.encodeToString(serializer.toBinary(s2))

      val noMetaColumnList = List(
        Value(StringValue(metadata.persistenceId)),
        Value(StringValue(metadata.sequenceNr.toString)), // ints and longs are StringValues :|
        Value(StringValue(unixTimestampMillisToSpanner(metadata.timestamp))),
        Value(StringValue(serializer.identifier.toString)),
        Value(StringValue(manifest)),
        Value(StringValue(snapshotBytes))
      )

      val columnList =
        metadata.metadata match {
          case Some(replicationMeta) =>
            val rm2 = replicationMeta.asInstanceOf[AnyRef]
            val metaSerializer = serialization.findSerializerFor(rm2)
            val metaManifest = Serializers.manifestFor(metaSerializer, rm2)
            val serializedMetaString =
              Base64.getEncoder.encodeToString(metaSerializer.toBinary(rm2))

            noMetaColumnList ++ List(
              Value(StringValue(serializedMetaString)),
              Value(StringValue(metaSerializer.identifier.toString)),
              Value(StringValue(metaManifest))
            )
          case None =>
            noMetaColumnList ++ List(
              Value(nullValue),
              Value(nullValue),
              Value(nullValue)
            )
        }

      spannerGrpcClient.withSession { implicit session =>
        if (log.isTraceEnabled()) {
          log.traceN(
            "Writing snapshot, persistenceId [{}], sequenceNr [{}], timestamp: [{}], session: [{}]",
            metadata.persistenceId,
            metadata.sequenceNr,
            metadata.timestamp,
            session.id
          )
        }
        spannerGrpcClient
          .write(
            Seq(
              Mutation(
                Mutation.Operation.Insert(
                  Mutation.Write(
                    settings.snapshotsTable,
                    Snapshots.Columns,
                    List(
                      ListValue(
                        columnList
                      )
                    )
                  )
                )
              )
            )
          )
          .recoverWith {
            case ex: StatusRuntimeException if ex.getStatus.getCode == Status.Code.ALREADY_EXISTS =>
              // We need to support upsert/overwrite of existing snapshot, but we
              // optimize based on that we most often do not update, we try write and update if that
              // fails because row exists we try an update
              log.debugN(
                "Snapshot already exists, updating. Session: {}, persistenceId: {}, sequenceNr: {}",
                session.id,
                metadata.persistenceId,
                metadata.sequenceNr
              )
              spannerGrpcClient
                .write(
                  Seq(
                    Mutation(
                      Mutation.Operation.Update(
                        Mutation.Write(
                          settings.snapshotsTable,
                          Snapshots.Columns,
                          List(
                            ListValue(
                              columnList
                            )
                          )
                        )
                      )
                    )
                  )
                )
          }
      }
    }

  def deleteSnapshots(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val query = s"DELETE FROM ${settings.snapshotsTable} " + wherePartFor(criteria)
    val queryParams = queryParamsFor(persistenceId, criteria)
    val queryParamTypes = queryParamTypesFor(criteria)

    spannerGrpcClient
      .withSession { implicit session =>
        if (log.isTraceEnabled()) {
          log.traceN(
            "deleteSnapshots: pid: {}, session: {}, criteria: {}, query: {}, params: {}, paramTypes: {}",
            persistenceId,
            session.id,
            criteria,
            query,
            queryParams,
            queryParamTypes
          )
        }

        spannerGrpcClient
          .executeBatchDml(
            List(
              (
                query,
                queryParams,
                queryParamTypes
              )
            )
          )
          .map(_ => ())(ExecutionContexts.parasitic)
      }
  }

  private def wherePartFor(criteria: SnapshotSelectionCriteria): String =
    "WHERE persistence_id = @persistence_id " +
    (if (criteria.maxSequenceNr != Long.MaxValue) "AND sequence_nr <= @max_seq_nr " else "") +
    (if (criteria.minSequenceNr > 0L) "AND sequence_nr >= @min_seq_nr " else "") +
    (if (criteria.maxTimestamp != Long.MaxValue) "AND timestamp <= @max_timestamp " else "") +
    (if (criteria.minTimestamp > 0L) "AND timestamp >= @min_timestamp " else "")

  private def queryParamsFor(persistenceId: String, criteria: SnapshotSelectionCriteria): Struct =
    Struct(
      Vector(
        Some(Snapshots.PersistenceId._1 -> Value(StringValue(persistenceId))),
        (if (criteria.maxSequenceNr != Long.MaxValue)
           Some("max_seq_nr" -> Value(StringValue(criteria.maxSequenceNr.toString)))
         else None),
        (if (criteria.minSequenceNr > 0L) Some("min_seq_nr" -> Value(StringValue(criteria.minSequenceNr.toString)))
         else None),
        (if (criteria.maxTimestamp != Long.MaxValue)
           Some("max_timestamp" -> Value(StringValue(unixTimestampMillisToSpanner(criteria.maxTimestamp))))
         else None),
        (if (criteria.minTimestamp > 0L)
           Some("min_timestamp" -> Value(StringValue(unixTimestampMillisToSpanner(criteria.minTimestamp))))
         else None)
      ).flatten.toMap
    )

  private def queryParamTypesFor(criteria: SnapshotSelectionCriteria): Map[String, Type] =
    Vector(
      Some("persistence_id" -> Type(TypeCode.STRING)),
      (if (criteria.maxSequenceNr != Long.MaxValue) Some("max_seq_nr" -> Type(TypeCode.INT64)) else None),
      (if (criteria.minSequenceNr > 0L) Some("min_seq_nr" -> Type(TypeCode.INT64)) else None),
      (if (criteria.maxTimestamp != Long.MaxValue) Some("max_timestamp" -> Type(TypeCode.TIMESTAMP)) else None),
      (if (criteria.minTimestamp > 0L) Some("min_timestamp" -> Type(TypeCode.TIMESTAMP)) else None)
    ).flatten.toMap
}
