/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.scaladsl

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.query.scaladsl._
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.persistence.spanner.internal.SpannerJournalInteractions.Schema
import akka.persistence.spanner.internal.{ContinuousQuery, SpannerGrpcClientExtension, SpannerJournalInteractions}
import akka.persistence.spanner.{SpannerOffset, SpannerSettings}
import akka.serialization.SerializationExtension
import akka.stream.scaladsl
import akka.stream.scaladsl.Source
import com.google.protobuf.struct.Value.Kind.StringValue
import com.google.protobuf.struct.{Struct, Value}
import com.google.spanner.v1.{Type, TypeCode}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.collection.immutable

object SpannerReadJournal {
  val Identifier = "akka.persistence.spanner.query"

  private val EventsByPersistenceIdTypes = Map(
    "persistence_id" -> Type(TypeCode.STRING),
    "from_sequence_nr" -> Type(TypeCode.INT64),
    "to_sequence_nr" -> Type(TypeCode.INT64)
  )
}

final class SpannerReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with CurrentEventsByTagQuery
    with EventsByTagQuery
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery {
  private val log = LoggerFactory.getLogger(classOf[SpannerReadJournal])
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = new SpannerSettings(system.settings.config.getConfig(sharedConfigPath))
  private val serialization = SerializationExtension(system)

  private val grpcClient = SpannerGrpcClientExtension(system.toTyped).clientFor(sharedConfigPath)

  private val EventsByTagSql =
    s"""SELECT ${SpannerJournalInteractions.Schema.Journal.Columns.map(column => s"j.$column").mkString(", ")}
       |FROM ${settings.eventTagTable} AS t JOIN ${settings.journalTable} AS j 
       |ON t.persistence_id = j.persistence_id AND t.sequence_nr = j.sequence_nr  
       |WHERE t.tag = @tag 
       |AND t.write_time >= @write_time 
       |ORDER BY t.write_time, t.persistence_id, t.sequence_nr""".stripMargin

  private val PersistenceIdsQuery =
    s"SELECT DISTINCT persistence_id from ${settings.journalTable}"

  private val EventsForPersistenceIdSql =
    s"SELECT ${Schema.Journal.Columns.mkString(",")} FROM ${settings.journalTable} WHERE persistence_id = @persistence_id AND sequence_nr >= @from_sequence_Nr AND sequence_nr <= @to_sequence_nr ORDER BY sequence_nr"

  override def currentEventsByTag(tag: String, offset: Offset): scaladsl.Source[EventEnvelope, NotUsed] = {
    val spannerOffset = toSpannerOffset(offset)
    log.debugN("Query from {}. From offset {}", spannerOffset.commitTimestamp, offset)
    grpcClient
      .streamingQuery(
        EventsByTagSql,
        params = Some(
          Struct(
            Map("tag" -> Value(StringValue(tag)), "write_time" -> Value(StringValue(spannerOffset.commitTimestamp)))
          )
        ),
        paramTypes = Map("tag" -> Type(TypeCode.STRING), "write_time" -> Type(TypeCode.TIMESTAMP))
      )
      .statefulMapConcat(deserializeAndAddOffset(spannerOffset))
      .mapMaterializedValue(_ => NotUsed)
  }

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    val initialOffset = toSpannerOffset(offset)

    def nextOffset(previousOffset: SpannerOffset, eventEnvelope: EventEnvelope): SpannerOffset =
      eventEnvelope.offset.asInstanceOf[SpannerOffset]

    ContinuousQuery[SpannerOffset, EventEnvelope](
      initialOffset,
      nextOffset,
      offset => Some(currentEventsByTag(tag, offset)),
      1, // the same row comes back and is filtered due to how the offset works
      settings.querySettings.refreshInterval
    )
  }

  private def toSpannerOffset(offset: Offset) = offset match {
    case NoOffset => SpannerOffset(Schema.Journal.NoOffset, Map.empty)
    case so: SpannerOffset => so
    case _ =>
      throw new IllegalArgumentException(s"Spanner Read Journal does not support offset type: " + offset.getClass)
  }

  override def currentPersistenceIds(): Source[String, NotUsed] = {
    log.debug("currentPersistenceIds")
    grpcClient
      .streamingQuery(PersistenceIdsQuery)
      .map { values =>
        values.head.getStringValue
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  override def persistenceIds(): Source[String, NotUsed] = {
    log.debug("persistenceIds")
    ContinuousQuery[Unit, String](
      (),
      (_, _) => (),
      _ => Some(currentPersistenceIds()),
      0,
      settings.querySettings.refreshInterval
    ).statefulMapConcat[String] { () =>
      var seenIds = Set.empty[String]
      pid => {
        if (seenIds.contains(pid)) Nil
        else {
          seenIds += pid
          pid :: Nil
        }
      }
    }
  }

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] =
    ContinuousQuery[Long, EventEnvelope](
      fromSequenceNr,
      (_, ee) => ee.sequenceNr,
      currentSequenceNr => {
        if (currentSequenceNr == toSequenceNr) {
          None
        } else {
          Some(currentEventsByPersistenceId(persistenceId, currentSequenceNr + 1, toSequenceNr))
        }
      },
      0,
      settings.querySettings.refreshInterval
    )

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] = {
    log.infoN("currentEventsByPersistenceId {} {} {}", persistenceId, fromSequenceNr, toSequenceNr)
    grpcClient
      .streamingQuery(
        EventsForPersistenceIdSql,
        params = Some(
          Struct(
            fields = Map(
              Schema.Journal.PersistenceId._1 -> Value(StringValue(persistenceId)),
              "from_sequence_nr" -> Value(StringValue(fromSequenceNr.toString)),
              "to_sequence_nr" -> Value(StringValue(toSequenceNr.toString))
            )
          )
        ),
        paramTypes = SpannerReadJournal.EventsByPersistenceIdTypes
      )
      .statefulMapConcat(deserializeAndAddOffset(SpannerOffset(Schema.Journal.NoOffset, Map.empty)))
      .mapMaterializedValue(_ => NotUsed)
  }

  // TODO Unit test in isolation
  private def deserializeAndAddOffset(
      spannerOffset: SpannerOffset
  ): () => Seq[Value] => immutable.Iterable[EventEnvelope] = { () =>
    var currentTimestamp: String = spannerOffset.commitTimestamp
    var currentSequenceNrs: Map[String, Long] = spannerOffset.seen
    row => {
      val (pr, commitTimestamp) = Schema.Journal.deserializeRow(serialization, row)
      if (commitTimestamp == currentTimestamp) {
        // has this already been seen?
        if (currentSequenceNrs.get(pr.persistenceId).exists(_ >= pr.sequenceNr)) {
          log.debugN(
            "filtering {} {} as commit timestamp is the same as last offset and is in seen {}",
            pr.persistenceId,
            pr.sequenceNr,
            currentSequenceNrs
          )
          Nil
        } else {
          currentSequenceNrs = currentSequenceNrs.updated(pr.persistenceId, pr.sequenceNr)
          List(
            EventEnvelope(
              SpannerOffset(commitTimestamp, currentSequenceNrs),
              pr.persistenceId,
              pr.sequenceNr,
              pr.payload,
              pr.timestamp
            )
          )
        }
      } else {
        // ne timestamp, reset currentSequenceNrs
        currentTimestamp = commitTimestamp
        currentSequenceNrs = Map(pr.persistenceId -> pr.sequenceNr)
        List(
          EventEnvelope(
            SpannerOffset(commitTimestamp, currentSequenceNrs),
            pr.persistenceId,
            pr.sequenceNr,
            pr.payload,
            pr.timestamp
          )
        )
      }
    }
  }
}
