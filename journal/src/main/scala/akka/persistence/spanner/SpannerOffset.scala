/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.persistence.query.Offset

/**
 * @param commitTimestamp microsecond granularity stored in nanoseconds
 * @param seen List of sequence nrs for every persistence id seen at this timestamp
 */
final case class SpannerOffset(
    commitTimestamp: String,
    seen: Map[String, Long]
) extends Offset
