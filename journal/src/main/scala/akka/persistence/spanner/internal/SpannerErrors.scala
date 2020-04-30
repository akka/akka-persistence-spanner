/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import io.grpc.Status
import io.grpc.Status.Code

/**
 * Some errors discovered by sending invalid requests to spanner
 */
sealed abstract class SpannerError(val statusCode: Status.Code, val startText: String)
case object TableNotFound extends SpannerError(Code.INVALID_ARGUMENT, "Table not found")
case object SessionDeleted extends SpannerError(Code.NOT_FOUND, "Session not found")
case object InvalidSession extends SpannerError(Code.INVALID_ARGUMENT, "Invalid")
case object DatabaseNotFound extends SpannerError(Code.NOT_FOUND, "Database not found")
