/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.javadsl

import akka.NotUsed
import akka.persistence.query.javadsl._
import akka.persistence.query.{EventEnvelope, Offset}
import akka.persistence.spanner.scaladsl
import akka.stream.javadsl.Source

object SpannerReadJournal {
  val Identifier = "akka.persistence.spanner.query"
}

final class SpannerReadJournal(delegate: scaladsl.SpannerReadJournal)
    extends ReadJournal
    with CurrentEventsByTagQuery
    with EventsByTagQuery
    with PersistenceIdsQuery
    with CurrentPersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery {
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    delegate.currentEventsByTag(tag, offset).asJava

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    delegate.eventsByTag(tag, offset).asJava

  override def persistenceIds(): Source[String, NotUsed] =
    delegate.persistenceIds().asJava

  override def currentPersistenceIds(): Source[String, NotUsed] =
    delegate.currentPersistenceIds().asJava

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] =
    delegate.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long
  ): Source[EventEnvelope, NotUsed] =
    delegate.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
}
