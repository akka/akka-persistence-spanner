/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.annotation.InternalApi
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.OptionVal
import akka.actor.typed.scaladsl.LoggerOps
import com.google.protobuf.struct.Value.Kind
import com.google.protobuf.struct.{ListValue, Value}
import com.google.spanner.v1.PartialResultSet
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 *
 * Recombines chunked and incomplete `PartialResultSet`s coming from upstream into rows
 * (represented by a `Seq` each) passed downstream
 */
@InternalApi private[akka] object RowCollector extends GraphStage[FlowShape[PartialResultSet, Seq[Value]]] {
  private val log = LoggerFactory.getLogger(getClass)
  // Custom graphstage avoids alloc per element in the happy case required by using statefulMapConcat
  val in = Inlet[PartialResultSet]("RowCollector.in")
  val out = Outlet[Seq[Value]]("RowCollector.out")

  private val MetadataNotSeenYet = -1

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      /* Rows can be split across several result sets but there can also be multiple
       * rows in one result set, and a combination of the two. Furthermore the split
       * of a row can happen mid field, indicated by the "chunked" flag.
       */

      private var incompleteRow: OptionVal[Seq[Value]] = OptionVal.None
      private var incompleteChunked = false
      private var columnsPerRow: Int = MetadataNotSeenYet

      override def onPush(): Unit = {
        val currentSet = grab(in)
        if (log.isTraceEnabled)
          log.traceN(
            "Chunk push, metadata seen: [{}], incompleteRows: [{}], incompleteChunked: [{}], columnsPerRow: [{}], " +
            "result set metadata: [{}], result set values [{}]",
            columnsPerRow != MetadataNotSeenYet,
            incompleteRow,
            incompleteChunked,
            columnsPerRow,
            currentSet.metadata,
            currentSet.values
          )
        if (columnsPerRow == MetadataNotSeenYet) {
          // first result set contains metadata about how many columns to expect
          // if not we got invalid data
          columnsPerRow = currentSet.metadata.get.rowType.get.fields.size
        }
        if (currentSet.values.nonEmpty) {
          incompleteRow match {
            case OptionVal.None =>
              val rows = currentSet.values.grouped(columnsPerRow).toSeq
              if (rows.last.size == columnsPerRow && !currentSet.chunkedValue) {
                emitMultiple(out, rows.iterator)
              } else {
                // first part of chunk or rows split across to the next set
                incompleteChunked = currentSet.chunkedValue
                incompleteRow = OptionVal.Some(rows.last)
                val initial = rows.init
                if (initial.nonEmpty) {
                  emitMultiple(out, initial.iterator)
                } else {
                  pull(in)
                }
              }

            case OptionVal.Some(incomplete) =>
              if (incompleteChunked) {
                // last was an incomplete value (chunked) - recombine
                val combined = recombine(incomplete, currentSet.values)
                val rows = combined.grouped(columnsPerRow).toSeq
                if (rows.last.size == columnsPerRow && !currentSet.chunkedValue) {
                  incompleteRow = OptionVal.None
                  emitMultiple(out, rows.iterator)
                } else {
                  // last row incomplete or chunked
                  incompleteChunked = currentSet.chunkedValue
                  incompleteRow = OptionVal.Some(rows.last)
                  val initial = rows.init
                  if (initial.nonEmpty)
                    emitMultiple(out, initial.iterator)
                  else {
                    pull(in)
                  }
                }
              } else {
                // last row incomplete or chunked
                val rows = (incomplete ++ currentSet.values).grouped(columnsPerRow).toSeq
                if (rows.last.size == columnsPerRow && !currentSet.chunkedValue) {
                  incompleteRow = OptionVal.None
                  emitMultiple(out, rows.iterator)
                } else {
                  // last incomplete or chunked
                  incompleteChunked = currentSet.chunkedValue
                  incompleteRow = OptionVal.Some(rows.last)
                  val initial = rows.init
                  if (initial.nonEmpty)
                    emitMultiple(out, initial.iterator)
                  else
                    pull(in)
                }
              }
          }
        } else {
          // empty resultset
          log.trace("Pulling and waiting for next result")
          pull(in)
        }
      }

      override def onPull(): Unit =
        pull(in)

      override def postStop(): Unit =
        if (incompleteRow.isDefined)
          log.warn("Stream stopped with incomplete result in buffer {}", incompleteRow)

      setHandlers(in, out, this)
    }

  def recombine(firstChunk: Seq[Value], secondChunk: Seq[Value]): Seq[Value] =
    (firstChunk.init :+ recombine(firstChunk.last, secondChunk.head)) ++ secondChunk.tail

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
              ListValue((beginning.values.init :+ recombine(beginning.values.last, end.values.head)) ++ end.values.tail)
            )
        }

      case Kind.StructValue(_) =>
        // will never be used in the spanner journal
        throw new IllegalArgumentException(s"Got a chunked ${part1.kind} value but that is not supported.")
      case Kind.BoolValue(_) | Kind.NumberValue(_) | Kind.NullValue(_) | Kind.Empty =>
        throw new IllegalArgumentException(s"Got a chunked ${part1.kind} value but that is not allowed.")
    })
}
