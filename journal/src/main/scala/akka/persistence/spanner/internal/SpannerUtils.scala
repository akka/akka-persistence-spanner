/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.time.Instant

import akka.annotation.InternalApi
import com.google.protobuf.struct.Value.Kind.NullValue

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object SpannerUtils {
  def unixTimestampMillisToSpanner(unixTimestampMs: Long): String =
    Instant.ofEpochMilli(unixTimestampMs).toString // I _think_ this will always be rfc3339

  def spannerTimestampToUnixMillis(spannerTimestamp: String): Long =
    Instant.parse(spannerTimestamp).toEpochMilli

  val nullValue: NullValue =
    com.google.protobuf.struct.Value.Kind.NullValue(com.google.protobuf.struct.NullValue.NULL_VALUE)
}
