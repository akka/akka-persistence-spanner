package akka.persistence.spanner.internal

import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.OptionVal
import com.google.protobuf.struct.Value.Kind
import com.google.protobuf.struct.{ListValue, Value}
import com.google.spanner.v1.PartialResultSet

/**
 * INTERNAL API
 */
@InternalApi object PartialResultSetFlow extends GraphStage[FlowShape[PartialResultSet, PartialResultSet]] {
  // Custom graphstage avoids alloc per element in the happy case required by using statefulMapConcat
  val in = Inlet[PartialResultSet](Logging.simpleName(this) + ".in")
  val out = Outlet[PartialResultSet](Logging.simpleName(this) + ".out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {
      private var previousChunk: OptionVal[PartialResultSet] = OptionVal.None

      override def onPush(): Unit = {
        val current = grab(in)
        previousChunk match {
          case OptionVal.Some(previous) =>
            // second part of chunk
            previousChunk = OptionVal.None
            val combined = recombine(previous, current)
            if (combined.chunkedValue) {
              previousChunk = OptionVal.Some(combined)
              pull(in)
            } else {
              push(out, combined)
            }
          case OptionVal.None =>
            if (current.chunkedValue) {
              // first part of chunk
              previousChunk = OptionVal.Some(current)
              pull(in)
            } else {
              // not chunked
              push(out, current)
            }
        }
      }

      override def onPull(): Unit =
        pull(in)

      override def postStop(): Unit =
        if (previousChunk.isDefined)
          log.warning("Stream stopped with first half of chunked result in buffer")

      setHandlers(in, out, this)
    }

  def recombine(firstChunk: PartialResultSet, secondChunk: PartialResultSet): PartialResultSet = {
    if (!firstChunk.chunkedValue)
      throw new IllegalArgumentException("Got unchunked value to recombine")

    val completeValues = firstChunk.values.init :+ recombine(firstChunk.values.last, secondChunk.values.head) :++ secondChunk.values.tail
    secondChunk.withValues(completeValues)
  }

  def recombine(part1: Value, part2: Value): Value =
    Value(part1.kind match {
      case Kind.StringValue(beginning) =>
        val Kind.StringValue(end) = part2.kind
        Kind.StringValue(beginning ++ end)
      case Kind.ListValue(beginning) =>
        val Kind.ListValue(end) = part2.kind
        // depending on type, do a recursive merge of the list items
        val beginningLast = beginning.values.last
        beginningLast.kind match {
          case Kind.NumberValue(_) | Kind.BoolValue(_) =>
            Kind.ListValue(ListValue(beginning.values ++ end.values))
          case _ =>
            Kind.ListValue(
              ListValue(beginning.values.init :+ recombine(beginning.values.last, end.values.head) :++ end.values.tail)
            )
        }

      case Kind.StructValue(_) =>
        // will never be used in the spanner journal
        throw new IllegalArgumentException(s"Got a chunked ${part1.kind} value but that is not supported.")
      case Kind.BoolValue(_) | Kind.NumberValue(_) | Kind.NullValue(_) | Kind.Empty =>
        throw new IllegalArgumentException(s"Got a chunked ${part1.kind} value but that is not allowed.")
    })
}
