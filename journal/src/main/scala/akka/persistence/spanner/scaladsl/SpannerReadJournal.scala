/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.scaladsl

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.query.scaladsl.{
  CurrentEventsByTagQuery,
  CurrentPersistenceIdsQuery,
  EventsByTagQuery,
  PersistenceIdsQuery,
  ReadJournal
}
import akka.persistence.query.{EventEnvelope, NoOffset, Offset}
import akka.persistence.spanner.internal.{ContinuousQuery, SpannerGrpcClientExtension}
import akka.persistence.spanner.{Schema, SpannerOffset, SpannerSettings}
import akka.serialization.SerializationExtension
import akka.stream.scaladsl
import akka.stream.scaladsl.Source
import com.google.protobuf.struct.Value.Kind.StringValue
import com.google.protobuf.struct.{Struct, Value}
import com.google.spanner.v1.{Type, TypeCode}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object SpannerReadJournal {
  val Identifier = "akka.persistence.spanner.query"
}

final class SpannerReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with CurrentEventsByTagQuery
    with EventsByTagQuery
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery {
  private val log = LoggerFactory.getLogger(classOf[SpannerReadJournal])
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = new SpannerSettings(system.settings.config.getConfig(sharedConfigPath))
  private val serialization = SerializationExtension(system)

  private val grpcClient = SpannerGrpcClientExtension(system.toTyped).clientFor(sharedConfigPath)

  private val EventsByTagSql =
    s"SELECT ${Schema.Journal.Columns.mkString(",")} from ${settings.table} WHERE @tag IN UNNEST(tags) AND write_time >= @write_time ORDER BY write_time, persistence_id, sequence_nr "

  private val PersistenceIdsQuery =
    s"SELECT DISTINCT persistence_id from ${settings.table}"

  override def currentEventsByTag(tag: String, offset: Offset): scaladsl.Source[EventEnvelope, NotUsed] = {
    val spannerOffset = toSpannerOffset(offset)
    log.debugN("Query from {}. From offset {}", spannerOffset.commitTimestamp, offset)
    grpcClient
      .streamingQuery(
        EventsByTagSql,
        params = Some(
          Struct(
            Map(
              "tag" -> Value(StringValue(tag)),
              "write_time" -> Value(StringValue(spannerOffset.commitTimestamp))
            )
          )
        ),
        paramTypes = Map(
          "tag" -> Type(TypeCode.STRING),
          "write_time" -> Type(TypeCode.TIMESTAMP)
        )
      )
      .statefulMapConcat { () =>
        {
          var currentTimestamp: String = spannerOffset.commitTimestamp
          var currentSequenceNrs: Map[String, Long] = spannerOffset.seen
          values => {
            val (pr, commitTimestamp) = Schema.Journal.deserializeRow(serialization, values)
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
      settings.queryConfig.refreshInterval
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
      settings.queryConfig.refreshInterval
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
}
