/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.javadsl

import akka.NotUsed
import akka.persistence.query.{EventEnvelope, Offset}
import akka.persistence.query.javadsl.{CurrentEventsByTagQuery, EventsByTagQuery, ReadJournal}
import akka.persistence.spanner.scaladsl
import akka.stream.javadsl.Source

object SpannerReadJournal {
  val Identifier = "akka.persistence.spanner.query"
}

final class SpannerReadJournal(delegate: scaladsl.SpannerReadJournal)
    extends ReadJournal
    with CurrentEventsByTagQuery
    with EventsByTagQuery {
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    delegate.currentEventsByTag(tag, offset).asJava

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    delegate.eventsByTag(tag, offset).asJava
}
