package akka.persistence.spanner.internal

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalField

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object SpannerUtils {
  def unixTimestampMillisToSpanner(unixTimestampMs: Long): String =
    Instant.ofEpochMilli(unixTimestampMs).toString // I _think_ this will always be rfc3339

  def spannerTimestampToUnixMillis(spannerTimestamp: String): Long =
    Instant.parse(spannerTimestamp).toEpochMilli
}
