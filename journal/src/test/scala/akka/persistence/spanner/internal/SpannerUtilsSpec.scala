/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import org.scalatest.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SpannerUtilsSpec extends AnyWordSpec with Matchers {
  val aVeryGoodTimeSpannerFormat = "2020-04-23T16:10:25.325Z"
  val aVeryGoodTimeUnixMillis = 1587658225325L
  "The Spanner utils" must {
    "format unix timestamps as rfc3339" in {
      SpannerUtils.unixTimestampMillisToSpanner(aVeryGoodTimeUnixMillis) should ===(aVeryGoodTimeSpannerFormat)
    }

    "parse spanner timestamps as rfc3339" in {
      SpannerUtils.spannerTimestampToUnixMillis(aVeryGoodTimeSpannerFormat) should ===(aVeryGoodTimeUnixMillis)
    }
  }
}
