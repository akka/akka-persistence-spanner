/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.spanner.internal.ContinuousQuery.NextQuery
import akka.stream.scaladsl.Source
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogicWithLogging}
import akka.util.OptionVal

import scala.concurrent.duration._

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object ContinuousQuery {
  def apply[S, T](
      initialState: S,
      updateState: (S, T) => S,
      nextQuery: S => Option[Source[T, NotUsed]],
      threshold: Long,
      refreshInterval: FiniteDuration
  ): Source[T, NotUsed] =
    Source.fromGraph(new ContinuousQuery[S, T](initialState, updateState, nextQuery, threshold, refreshInterval))

  private case object NextQuery
  private case object Status
}

/**
 * INTERNAL API
 *
 * Keep running the Source's returned by `nextQuery` until None is returned
 * Each time updating the state
 * @param initialState Initial state for first call to nextQuery
 * @param updateState  Called for every element
 * @param nextQuery Called each time the previous source completes
 * @tparam S State type
 * @tparam T Element type
 */
@InternalApi
final private[spanner] class ContinuousQuery[S, T](
    initialState: S,
    updateState: (S, T) => S,
    nextQuery: S => Option[Source[T, NotUsed]],
    threshold: Long,
    refreshInterval: FiniteDuration
) extends GraphStage[SourceShape[T]] {
  val out = Outlet[T]("spanner.out")
  override def shape: SourceShape[T] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogicWithLogging(shape) with OutHandler {
      var nextRow: OptionVal[T] = OptionVal.none[T]
      var sinkIn: SubSinkInlet[T] = _
      var state = initialState
      var nrElements = Long.MaxValue
      var subStreamFinished = false

      def pushAndUpdateState(t: T) = {
        state = updateState(state, t)
        nrElements += 1
        log.debug("pushing {}", t)
        push(out, t)
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case NextQuery => next()
        case ContinuousQuery.Status =>
          log.info(
            "Status: has been pulled? {}. subStreamFinished {}. innerSink has been pulled? {}, inner sink closed {}",
            isAvailable(out),
            subStreamFinished,
            sinkIn.hasBeenPulled,
            sinkIn.isClosed
          )
      }

      def next(): Unit =
        if (nrElements <= threshold) {
          log.debug("Scheduling next query for later")
          nrElements = Long.MaxValue
          scheduleOnce(NextQuery, refreshInterval)
        } else {
          log.debug("Running next query now")
          nrElements = 0
          subStreamFinished = false
          val source = nextQuery(state)
          log.debug("Next source {}. Current state {} {}", source, nextRow, state)
          source match {
            case Some(source) =>
              sinkIn = new SubSinkInlet[T]("Inner")
              sinkIn.setHandler(new InHandler {
                override def onPush(): Unit = {
                  log.debug("onPush inner")
                  if (isAvailable(out)) {
                    if (!nextRow.isEmpty) {
                      throw new RuntimeException(s"onPush called when we already have: " + nextRow)
                    }
                    val element = sinkIn.grab()
                    log.debug("onPush inner pushing right away {}", element)
                    pushAndUpdateState(element)
                    sinkIn.pull()
                  } else {
                    if (!nextRow.isEmpty) {
                      throw new RuntimeException(s"onPush called when we already have: " + nextRow)
                    }
                    log.debug("onPush inner buffering element, not pulling until it is taken")
                    nextRow = OptionVal(sinkIn.grab())
                  }
                }

                override def onUpstreamFinish(): Unit =
                  if (nextRow.isDefined) {
                    log.debug("Stream finished. Not creating next as a buffered element: {}", nextRow)
                    // wait for the element to be pulled
                    subStreamFinished = true
                  } else {
                    log.debug("Stream finished. Creating next. " + nextRow)
                    next()
                  }
              })
              val graph = Source
                .fromGraph(source)
                .to(sinkIn.sink)
              interpreter.subFusingMaterializer.materialize(graph)
              sinkIn.pull()
            case None =>
              log.debug("Completing stage")
              completeStage()
          }
        }

      override def preStart(): Unit = {
        scheduleAtFixedRate(ContinuousQuery.Status, 400.millis, 400.millis)
        next()
      }

      override def onPull(): Unit = {
        log.debug("onPull. Buffered row: {}", nextRow)
        nextRow match {
          case OptionVal.Some(row) =>
            pushAndUpdateState(row)
            nextRow = OptionVal.none[T]
            if (subStreamFinished) {
              next()
            } else {
              log.info("I should really pull shouldn't i? {} {}", sinkIn.hasBeenPulled, sinkIn.isClosed)
              if (!sinkIn.isClosed && !sinkIn.hasBeenPulled) {
                log.info("should have pulled")
              }
            }
          case OptionVal.None =>
            if (!subStreamFinished && !sinkIn.isClosed && !sinkIn.hasBeenPulled) {
              log.debug("onPull and no element, pulling")
              sinkIn.pull()
            }
        }
      }

      setHandler(out, this)
    }
}
