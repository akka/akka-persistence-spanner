/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.time.{Instant, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.Base64

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.PersistentRepr
import akka.persistence.spanner.internal.SessionPool.PooledSession
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SpannerJournalInteractions.SerializedWrite
import akka.serialization.Serialization
import akka.util.ConstantFun
import com.google.protobuf.struct.Value.Kind.StringValue
import com.google.protobuf.struct.{ListValue, Struct, Value}
import com.google.spanner.v1.{Mutation, Type, TypeCode}

import scala.concurrent.{ExecutionContext, Future}

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object SpannerJournalInteractions {
  case class SerializedWrite(
      persistenceId: String,
      sequenceNr: Long,
      payload: String,
      serId: Long,
      serManifest: String,
      writerUuid: String,
      tags: Set[String]
  )

  object Schema {
    object Journal {
      def journalTable(settings: SpannerSettings): String =
        s"""CREATE TABLE ${settings.journalTable} (
           |  persistence_id STRING(MAX) NOT NULL,
           |  sequence_nr INT64 NOT NULL,
           |  event BYTES(MAX),
           |  ser_id INT64 NOT NULL,
           |  ser_manifest STRING(MAX) NOT NULL,
           |  write_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
           |  writer_uuid STRING(MAX) NOT NULL,
           |) PRIMARY KEY (persistence_id, sequence_nr)""".stripMargin

      val PersistenceId = "persistence_id" -> Type(TypeCode.STRING)
      val SeqNr = "sequence_nr" -> Type(TypeCode.INT64)
      val Event = "event" -> Type(TypeCode.BYTES)
      val SerId = "ser_id" -> Type(TypeCode.INT64)
      val SerManifest = "ser_manifest" -> Type(TypeCode.STRING)
      val WriteTime = "write_time" -> Type(TypeCode.TIMESTAMP)
      val WriterUUID = "writer_uuid" -> Type(TypeCode.STRING)

      val Columns = List(PersistenceId, SeqNr, Event, SerId, SerManifest, WriteTime, WriterUUID).map(_._1)

      val formatter = (new DateTimeFormatterBuilder)
        .appendOptional(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        .optionalStart
        .appendOffsetId
        .optionalEnd
        .toFormatter
        .withZone(ZoneOffset.UTC)

      val NoOffset = SpannerUtils.unixTimestampMillisToSpanner(0L)

      val ReplayTypes = Map(
        "persistence_id" -> Type(TypeCode.STRING),
        "from_sequence_nr" -> Type(TypeCode.INT64),
        "to_sequence_nr" -> Type(TypeCode.INT64),
        "max" -> Type(TypeCode.INT64)
      )

      def deserializeRow(serialization: Serialization, values: Seq[Value]): (PersistentRepr, String) = {
        val iterator = values.iterator
        val persistenceId = iterator.next.getStringValue
        val sequenceNr = iterator.next.getStringValue.toLong
        val payloadAsString = iterator.next.getStringValue
        val serId = iterator.next.getStringValue.toInt
        val serManifest = iterator.next.getStringValue
        // keep this in the offset as the original format rather than do any conversions
        val writeOriginal = iterator.next.getStringValue
        val writeTimestamp: Long = SpannerUtils.spannerTimestampToUnixMillis(writeOriginal)
        val writerUuid = iterator.next.getStringValue
        val payloadAsBytes = Base64.getDecoder.decode(payloadAsString)
        val payload = serialization.deserialize(payloadAsBytes, serId, serManifest).get
        (
          PersistentRepr(payload, sequenceNr, persistenceId, writerUuid = writerUuid)
            .withTimestamp(writeTimestamp),
          writeOriginal
        )
      }
    }

    object Tags {
      def tagTable(settings: SpannerSettings): String =
        s"""CREATE TABLE ${settings.eventTagTable} (
           |  persistence_id STRING(MAX) NOT NULL,
           |  sequence_nr INT64 NOT NULL,
           |  tag STRING(MAX) NOT NULL,
           |  write_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
           |) PRIMARY KEY (persistence_id, sequence_nr, tag),
           |INTERLEAVE IN PARENT ${settings.journalTable} ON DELETE CASCADE""".stripMargin

      def eventsByTagIndex(settings: SpannerSettings): String =
        s"""CREATE INDEX ${settings.eventTagTable}_tag_and_offset
           |ON ${settings.eventTagTable} (
           |  tag,
           |  write_time
           |)""".stripMargin

      val PersistenceId = "persistence_id" -> Type(TypeCode.STRING)
      val SeqNr = "sequence_nr" -> Type(TypeCode.INT64)
      val Tag = "tag" -> Type(TypeCode.STRING)
      val WriteTime = "write_time" -> Type(TypeCode.TIMESTAMP)

      val Columns = List(PersistenceId, SeqNr, Tag, WriteTime).map(_._1)
    }

    object Deleted {
      def deleteMetadataTable(settings: SpannerSettings): String =
        s"""CREATE TABLE ${settings.deletionsTable} (
           |  persistence_id STRING(MAX) NOT NULL,
           |  deleted_to INT64 NOT NULL,
           |) PRIMARY KEY (persistence_id)""".stripMargin

      val DeleteStatementTypes: Map[String, Type] =
        Map("persistence_id" -> Type(TypeCode.STRING), "sequence_nr" -> Type(TypeCode.INT64))
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
private[spanner] class SpannerJournalInteractions(
    spannerGrpcClient: SpannerGrpcClient,
    journalSettings: SpannerSettings
)(implicit ec: ExecutionContext, system: ActorSystem) {
  import SpannerJournalInteractions.Schema
  import Schema._

  val log = Logging(system, classOf[SpannerJournalInteractions])

  val HighestDeleteSelectSql =
    s"SELECT deleted_to FROM ${journalSettings.deletionsTable} WHERE persistence_id = @persistence_id"

  val HighestSequenceNrSql =
    s"SELECT sequence_nr FROM ${journalSettings.journalTable} WHERE persistence_id = @persistence_id AND sequence_nr >= @sequence_nr ORDER BY sequence_nr DESC LIMIT 1"

  val ReplaySql =
    s"SELECT ${Journal.Columns.mkString(",")} FROM ${journalSettings.journalTable} WHERE persistence_id = @persistence_id AND sequence_nr >= @from_sequence_Nr AND sequence_nr <= @to_sequence_nr ORDER BY sequence_nr limit @max"

  val SqlDeleteInsertToDeletions =
    s"INSERT INTO ${journalSettings.deletionsTable}(persistence_id, deleted_to) VALUES (@persistence_id, @sequence_nr)"

  val SqlDelete =
    s"DELETE FROM ${journalSettings.journalTable} where persistence_id = @persistence_id AND sequence_nr <= @sequence_nr"

  def writeEvents(events: Seq[SerializedWrite]): Future[Unit] = {
    val mutations = events.flatMap { sw =>
      val eventMutation = Mutation(
        Mutation.Operation.Insert(
          Mutation.Write(
            journalSettings.journalTable,
            Journal.Columns,
            List(
              ListValue(
                List(
                  Value(StringValue(sw.persistenceId)),
                  Value(StringValue(sw.sequenceNr.toString)), // ints and longs are StringValues :|
                  Value(StringValue(sw.payload)),
                  Value(StringValue(sw.serId.toString)),
                  Value(StringValue(sw.serManifest)),
                  // special value for a timestamp that gets the write timestamp
                  Value(StringValue("spanner.commit_timestamp()")),
                  Value(StringValue(sw.writerUuid))
                )
              )
            )
          )
        )
      )
      if (sw.tags.isEmpty) {
        eventMutation :: Nil
      } else {
        val serializedTags: List[Mutation] = sw.tags.toList.map(
          tagName =>
            Mutation(
              Mutation.Operation.Insert(
                Mutation.Write(
                  journalSettings.eventTagTable,
                  Tags.Columns,
                  List(
                    ListValue(
                      List(
                        Value(StringValue(sw.persistenceId)),
                        Value(StringValue(sw.sequenceNr.toString)), // ints and longs are StringValues :|
                        Value(StringValue(tagName)),
                        // special value for a timestamp that gets the write timestamp
                        Value(StringValue("spanner.commit_timestamp()"))
                      )
                    )
                  )
                )
              )
            )
        )

        eventMutation :: serializedTags
      }
    }

    spannerGrpcClient.withSession { session =>
      log.debug("writeEvents, session id [{}]", session.id)
      spannerGrpcClient.write(mutations)(session)
    }
  }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    spannerGrpcClient.withSession(implicit session => internalReadHighestSequenceNr(persistenceId, fromSequenceNr))

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    spannerGrpcClient.withSession { implicit session =>
      log.debug("deleteMessagesTo, session id [{}]", session.id)

      def params(to: Long) =
        Struct(
          Map("persistence_id" -> Value(StringValue(persistenceId)), "sequence_nr" -> Value(StringValue(to.toString)))
        )
      for {
        highestDeletedTo <- findHighestDeletedTo(persistenceId) // user may have passed in a smaller value than previously deleted
        toDeleteTo <- {
          if (toSequenceNr == Long.MaxValue) { // special to delete all but don't set max deleted to it
            internalReadHighestSequenceNr(persistenceId, highestDeletedTo)
          } else {
            Future.successful(math.max(highestDeletedTo, toSequenceNr))
          }
        }
        _ <- spannerGrpcClient.executeBatchDml(
          List(
            (SqlDelete, params(toDeleteTo), Deleted.DeleteStatementTypes),
            (SqlDeleteInsertToDeletions, params(toDeleteTo), Deleted.DeleteStatementTypes)
          )
        )
      } yield ()
    }

  def streamJournal(
      serialization: Serialization,
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long
  )(replay: PersistentRepr => Unit): Future[Unit] = {
    def replayRow(values: Seq[Value]): Unit = {
      val (repr, _) = Schema.Journal.deserializeRow(serialization, values)
      log.debug("replaying {}", repr)
      replay(repr)
    }

    spannerGrpcClient
      .streamingQuery(
        ReplaySql,
        params = Some(
          Struct(
            fields = Map(
              Journal.PersistenceId._1 -> Value(StringValue(persistenceId)),
              "from_sequence_nr" -> Value(StringValue(fromSequenceNr.toString)),
              "to_sequence_nr" -> Value(StringValue(toSequenceNr.toString)),
              "max" -> Value(StringValue(max.toString))
            )
          )
        ),
        paramTypes = Schema.Journal.ReplayTypes
      )
      .runForeach(replayRow)
      .map(ConstantFun.scalaAnyToUnit)
  }

  private def findHighestDeletedTo(persistenceId: String)(implicit session: PooledSession): Future[Long] =
    spannerGrpcClient
      .executeQuery(
        HighestDeleteSelectSql,
        Struct(Map(Journal.PersistenceId._1 -> Value(StringValue(persistenceId)))),
        Map(Journal.PersistenceId)
      )
      .map { resultSet =>
        if (resultSet.rows.isEmpty) 0L
        else resultSet.rows.head.values.head.getStringValue.toLong
      }

  private def internalReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long)(
      implicit session: PooledSession
  ): Future[Long] = {
    val maxDeletedTo: Future[Long] = findHighestDeletedTo(persistenceId)
    val maxSequenceNr: Future[Long] = spannerGrpcClient
      .executeQuery(
        HighestSequenceNrSql,
        Struct(
          Map(
            Journal.PersistenceId._1 -> Value(StringValue(persistenceId)),
            Journal.SeqNr._1 -> Value(StringValue(fromSequenceNr.toString))
          )
        ),
        Map(Journal.PersistenceId, Journal.SeqNr)
      )
      .map(
        resultSet =>
          resultSet.rows.size match {
            case 0 =>
              log.debug("No rows for persistence id [{}], using fromSequenceNr [{}]", persistenceId, fromSequenceNr)
              fromSequenceNr
            case 1 =>
              val sequenceNr = resultSet.rows.head.values.head.getStringValue.toLong
              log.debug("Single row. {}", sequenceNr)
              sequenceNr
            case _ => throw new RuntimeException("More than one row returned from a limit 1 query. " + resultSet)
          }
      )

    for {
      deletedTo <- maxDeletedTo
      max <- maxSequenceNr
    } yield {
      log.debug("Max deleted to [{}] max sequence nr [{}]", deletedTo, max)
      math.max(deletedTo, max)
    }
  }
}
