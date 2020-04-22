/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.javadsl

import akka.persistence.query.javadsl.ReadJournal
import akka.persistence.spanner.scaladsl

object SpannerReadJournal {
  val Identifier = "akka.persistence.spanner.query"
}

final class SpannerReadJournal(delegate: scaladsl.SpannerReadJournal) extends ReadJournal {}
