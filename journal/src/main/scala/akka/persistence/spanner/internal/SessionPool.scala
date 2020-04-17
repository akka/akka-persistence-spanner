/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal
import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.annotation.InternalApi
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool.{Command, GetSession, PoolBusy, PooledSession, ReleaseSession}
import com.google.spanner.v1.{BatchCreateSessionsRequest, Session, SpannerClient}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success}

/**
 *
 * INTERNAL API
 *
 * Sessions are used to execute transactions
 * They are expensive so should be re-used.
 *
 * TODO:
 *  - Keep a live or re-creation, by default they are closed on the server if not used for 1 hour, can send a select 1 to keep alive
 *  - adaptable size
 *  - deal with broken session
 *  - exponential back off for session creation
 *
 * See google advice for session pools: https://cloud.google.com/spanner/docs/sessions#create_and_size_the_session_cache
 *
 */
@InternalApi
private[spanner] object SessionPool {
  sealed trait Command
  final case class GetSession(replyTo: ActorRef[Response], id: UUID) extends Command
  final case class ReleaseSession(id: UUID) extends Command

  private final case class InitialSessions(sessions: List[Session]) extends Command
  private final case class RetrySessionCreation(in: FiniteDuration) extends Command

  sealed trait Response {
    val id: UUID
  }
  final case class PooledSession(session: Session, id: UUID) extends Response
  final case class PoolBusy(id: UUID) extends Response

  def apply(client: SpannerClient, settings: SpannerSettings): Behavior[SessionPool.Command] =
    Behaviors.withStash[Command](settings.sessionPool.maxOutstandingRequests) { stash =>
      Behaviors.withTimers[Command] { timers: TimerScheduler[Command] =>
        Behaviors.setup[Command] { ctx =>
          ctx.log.info(
            "Creating pool. Max size {}. Stash {}.",
            settings.sessionPool.maxSize,
            settings.sessionPool.maxOutstandingRequests
          )
          def createSessions(): Unit = {
            ctx.log.info("Creating sessions {} {}", settings.fullyQualifiedDatabase, settings.sessionPool.maxSize)
            ctx.pipeToSelf(
              client.batchCreateSessions(
                BatchCreateSessionsRequest(settings.fullyQualifiedDatabase, sessionCount = settings.sessionPool.maxSize)
              )
            ) {
              case Success(response) =>
                ctx.log.info("Sessions created {}", response)
                InitialSessions(response.session.toList)
              case Failure(t) =>
                ctx.log.warn("Session creation failed. Retrying ", t)
                RetrySessionCreation(settings.sessionPool.retryCreateInterval)
            }
          }

          createSessions()

          Behaviors.receiveMessagePartial {
            case InitialSessions(sessions) =>
              stash.unstashAll(new SessionPool(sessions, ctx, timers, stash))
            case RetrySessionCreation(when) =>
              if (when == Duration.Zero) {
                ctx.log.info("Retrying session creation")
                createSessions()
              } else {
                ctx.scheduleOnce(when, ctx.self, RetrySessionCreation(Duration.Zero))
              }
              Behaviors.same
            case gt@GetSession(replyTo, id) =>
              if (stash.isFull) {
                replyTo ! PoolBusy(id)
              } else {
                stash.stash(gt)
              }
              Behaviors.same
          }
        }
      }
    }
}

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] final class SessionPool(
    initialSessions: List[Session],
    ctx: ActorContext[Command],
    timers: TimerScheduler[Command],
    stash: StashBuffer[Command]
) extends AbstractBehavior[SessionPool.Command](ctx) {
  private val log = ctx.log
  private var availableSessions = initialSessions
  private var inUseSessions = Map.empty[UUID, Session]

  override def onMessage(msg: Command): Behavior[Command] = msg match {
    case gt@GetSession(replyTo, id) =>
      log.info("GetSession from {} in-use {} available{}", replyTo, inUseSessions, availableSessions)
      availableSessions match {
        case x :: xs =>
          replyTo ! PooledSession(x, id)
          availableSessions = xs
          inUseSessions += (id -> x)
        case Nil =>
          if (stash.isFull) {
            replyTo ! PoolBusy(id)
          } else {
            stash.stash(gt)
          }
      }
      this
    case ReleaseSession(id) =>
      log.info("ReleaseSession {}", id)
      if (inUseSessions.contains(id)) {
        val session = inUseSessions(id)
        inUseSessions -= id
        availableSessions = session :: availableSessions
        stash.unstash(this, 1, identity)
      } else {
        log.error(
          "unknown session returned {}. This is a bug. In-use sessions {}. Available sessions {}",
          id,
          inUseSessions,
          availableSessions
        )
        this
      }
    case other =>
      log.error("Unexpected message in active state {}", other)
      this
  }
}
