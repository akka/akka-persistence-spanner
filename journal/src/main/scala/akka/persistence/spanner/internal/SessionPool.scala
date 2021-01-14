/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.Done
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, LoggerOps, TimerScheduler}
import akka.actor.typed._
import akka.annotation.InternalApi
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool._
import akka.util.PrettyDuration._
import com.google.spanner.v1._
import io.grpc.StatusRuntimeException
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 *
 * INTERNAL API
 *
 * Sessions are used to execute transactions
 * They are expensive so should be re-used.
 *
 * See https://github.com/akka/akka-persistence-spanner/issues/12 for future features
 *
 * See google advice for session pools: https://cloud.google.com/spanner/docs/sessions#create_and_size_the_session_cache
 *
 */
@InternalApi
private[spanner] object SessionPool {
  sealed trait Command
  final case class GetSession(replyTo: ActorRef[Response], id: Long) extends Command
  final case class ReleaseSession(id: Long, recreate: Boolean) extends Command
  final case class Shutdown(done: Promise[Done]) extends Command
  private final case object ShutdownTimeout extends Command
  private final case class InitialSessions(sessions: List[Session]) extends Command
  private final case class RetrySessionCreation(in: FiniteDuration) extends Command
  private final case object CreateSingleSession extends Command
  private final case class SessionCreated(session: Session) extends Command
  private final case class SessionCreateFailed(t: Throwable) extends Command
  private case object KeepAlive extends Command
  private case object Stats extends Command

  sealed trait Response {
    val id: Long
  }
  final case class PooledSession(session: Session, id: Long) extends Response
  final case class PoolBusy(id: Long) extends Response
  final case class PoolShuttingDown(id: Long) extends Response

  private final case class SessionKeepAliveSuccess(session: Session) extends Command
  private final case class SessionKeepAliveFailure(session: Session, cause: Throwable) extends Command

  private final case class AvailableSession(session: Session, lastUsed: Long)

  def apply(client: SpannerClient, settings: SpannerSettings): Behavior[SessionPool.Command] =
    Behaviors.withStash[Command](settings.sessionPool.maxOutstandingRequests) { stash =>
      Behaviors.withTimers[Command] { timers =>
        Behaviors.setup[Command] { ctx =>
          ctx.log.info(
            "Creating pool. Max size [{}]. Max outstanding requests [{}].",
            settings.sessionPool.maxSize,
            settings.sessionPool.maxOutstandingRequests
          )
          def createSessions(): Unit = {
            ctx.log.debug(
              "Creating sessions, database [{}], pool size [{}]",
              settings.fullyQualifiedDatabase,
              settings.sessionPool.maxSize
            )
            ctx.pipeToSelf(
              client.batchCreateSessions(
                BatchCreateSessionsRequest(settings.fullyQualifiedDatabase, sessionCount = settings.sessionPool.maxSize)
              )
            ) {
              case Success(response) => InitialSessions(response.session.toList)
              case Failure(t) =>
                ctx.log.warn("Session creation failed. Retrying ", t)
                RetrySessionCreation(settings.sessionPool.retryCreateInterval)
            }
          }

          createSessions()

          Behaviors.receiveMessage {
            case InitialSessions(sessions) =>
              ctx.log.debug("Sessions created [{}]", sessions)
              timers.startTimerWithFixedDelay(KeepAlive, settings.sessionPool.keepAliveInterval)
              settings.sessionPool.statsInternal.foreach(duration => timers.startTimerWithFixedDelay(Stats, duration))
              stash.unstashAll(
                new SessionPool(client, sessions, ctx, timers, settings)
              )
            case RetrySessionCreation(when) =>
              if (when == Duration.Zero) {
                ctx.log.debug("Retrying session creation")
                createSessions()
              } else {
                timers.startSingleTimer(RetrySessionCreation(Duration.Zero), when)
              }
              Behaviors.same
            case gt @ GetSession(replyTo, id) =>
              if (stash.isFull) {
                ctx.log.warn("Session pool request stash full, denying request for pool while starting up")
                replyTo ! PoolBusy(id)
              } else {
                stash.stash(gt)
              }
              Behaviors.same
            case other =>
              stash.stash(other)
              Behaviors.same
          }
        }
      }
    }

  def shuttingDown(
      done: Promise[Done],
      client: SpannerClient,
      idleSessions: List[Session],
      remainingSessions: Map[Long, Session]
  ): Behavior[Command] =
    Behaviors.setup { ctx =>
      Behaviors.receiveMessagePartial {
        case GetSession(replyTo, id) =>
          replyTo ! PoolShuttingDown(id)
          Behaviors.same
        case ReleaseSession(id, _) =>
          val newSessions = remainingSessions.get(id).toList ++ idleSessions
          val newRemaining = remainingSessions - id
          if (newRemaining.isEmpty) {
            if (ctx.log.isInfoEnabled) {
              ctx.log.info("All sessions returned. Shutting down. {}", newSessions.map(_.name))
            }
            Behaviors.stopped(() => {
              cleanupOldSessions(client, newSessions)
              done.tryComplete(Success(Done))
            })
          } else {
            ctx.log.info("Still waiting on sessions to return: {}", newRemaining.keys)
            shuttingDown(done, client, newSessions, newRemaining)
          }
        case ShutdownTimeout =>
          val toShutdown = idleSessions ++ remainingSessions.values
          if (ctx.log.isInfoEnabled) {
            ctx.log.info("Timed out waiting for sessions to be returned. Shutting down now. {}", toShutdown.map(_.name))
          }
          Behaviors.stopped(() => {
            cleanupOldSessions(client, toShutdown)
            done.tryComplete(Success(Done))
          })
      }
    }

  private def cleanupOldSessions(client: SpannerClient, sessions: Seq[Session]): Unit =
    sessions.foreach { session =>
      client.deleteSession(DeleteSessionRequest(session.name))
    }
}

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] final class SessionPool(
    client: SpannerClient,
    initialSessions: List[Session],
    ctx: ActorContext[Command],
    timers: TimerScheduler[Command],
    settings: SpannerSettings
) extends AbstractBehavior[SessionPool.Command](ctx) {
  private val statsLogger = LoggerFactory.getLogger(settings.sessionPool.statsLogger)
  private val keepAliveInMillis = settings.sessionPool.keepAliveInterval.toMillis
  private val log = ctx.log
  private var availableSessions: mutable.Queue[AvailableSession] =
    mutable.Queue(initialSessions.map(AvailableSession(_, System.currentTimeMillis())): _*)
  private var inUseSessions = Map.empty[Long, Session]
  private var uses = 0
  private var requestQueue: mutable.Queue[GetSession] = mutable.Queue()

  override def onMessage(msg: Command): Behavior[Command] = msg match {
    case gt @ GetSession(replyTo, id) =>
      if (log.isTraceEnabled()) {
        log.traceN(
          "GetSession [{}] from [{}], inUseSessions [{}], availableSessions [{}], queued [{}]",
          id,
          replyTo,
          inUseSessions.mkString(", "),
          availableSessions.map(a => (a.session.name, a.lastUsed)).mkString(", "),
          requestQueue.size
        )
      }
      if (availableSessions.nonEmpty) {
        handOutSession(id, replyTo)
      } else {
        if (requestQueue.size >= settings.sessionPool.maxOutstandingRequests) {
          log.warn("Session pool request queue full, denying request for pool")
          replyTo ! PoolBusy(id)
        } else {
          log.trace("No free sessions, enqueuing request for session [{}]", id)
          requestQueue.enqueue(gt)
        }
      }
      this
    case ReleaseSession(id, shouldRecreate) =>
      uses += 1
      if (inUseSessions.contains(id)) {
        val session = inUseSessions(id)
        log.trace("Session [{}], [{}] released", id, session.name)
        inUseSessions -= id
        if (!shouldRecreate) {
          availableSessions.enqueue(AvailableSession(session, System.currentTimeMillis()))
          handOutSessionToQueuedRequest()
        } else {
          log.debugN(
            "Session released with should re-create. Not returning this session to the pool [{}] (all [{}])",
            session.name
          )
          ctx.self ! CreateSingleSession
        }
      } else if (requestQueue.exists(_.id == id)) {
        // requestor gave up before they got a session
        log.trace("Queued session request [{}] given up without getting a session", id)
        requestQueue = requestQueue.filterNot(_.id == id)
      } else {
        log.error("Unknown session returned [{}], this is a bug", id)
        if (log.isDebugEnabled) {
          log.debugN(
            "In-use sessions [{}]. Available sessions [{}]",
            inUseSessions.map { case (id, session) => (id, session.name) }.mkString(", "),
            availableSessions
              .map { case AvailableSession(session, lastUsed) => (session.name, lastUsed) }
              .mkString(", ")
          )
        }
      }
      this
    case KeepAlive =>
      val currentTime = System.currentTimeMillis()
      val toKeepAlive = availableSessions.collect {
        case AvailableSession(session, lastUse) if (currentTime - lastUse) > keepAliveInMillis =>
          session
      }

      if (toKeepAlive.nonEmpty) {
        if (log.isDebugEnabled) {
          log.debugN(
            "The following sessions haven't been used in the last {}. Sending keep alive. [{}]",
            settings.sessionPool.keepAliveInterval.pretty,
            toKeepAlive.map(_.name)
          )
        }
        availableSessions = availableSessions.filterNot(s => toKeepAlive.contains(s.session))

        toKeepAlive.foreach { session =>
          ctx.pipeToSelf(client.executeSql(ExecuteSqlRequest(session.name, sql = "SELECT 1"))) {
            case Success(_) => SessionKeepAliveSuccess(session)
            case Failure(t) => SessionKeepAliveFailure(session, t)
          }
        }
      }
      this

    case SessionKeepAliveSuccess(session) =>
      log.trace("Session keep alive successful for [{}]", session.name)
      availableSessions.enqueue(AvailableSession(session, System.currentTimeMillis()))
      handOutSessionToQueuedRequest()
      this

    case SessionKeepAliveFailure(session, cause) =>
      cause match {
        case t: StatusRuntimeException if t.getStatus.getCode == SessionDeleted.statusCode =>
          log.warn(
            "Keep alive of session failed with NOT_FOUND. Re-creating session and adding new instance to pool.",
            t
          )
          context.self ! CreateSingleSession
        case _ =>
          log.warn(
            s"Failed to keep session [${session.name}] alive, may be re-tried again before expires server side. " +
            s"Only Status.NOT_FOUND causes a session to be re-created.",
            cause
          )
          availableSessions.enqueue(AvailableSession(session, System.currentTimeMillis()))
          handOutSessionToQueuedRequest()
      }
      this

    case CreateSingleSession =>
      context.pipeToSelf(client.createSession(CreateSessionRequest(settings.fullyQualifiedDatabase))) {
        case Success(s) => SessionCreated(s)
        case Failure(t) => SessionCreateFailed(t)
      }
      this

    case SessionCreated(s) =>
      log.debug("Replacement session created [{}]", s)
      availableSessions.enqueue(AvailableSession(s, System.currentTimeMillis()))
      handOutSessionToQueuedRequest()
      this

    case SessionCreateFailed(t) =>
      log.warn("Failed to create replacement session. It will be retried.", t)
      timers.startSingleTimer(CreateSingleSession, settings.sessionPool.retryCreateInterval)
      this

    case Stats =>
      statsLogger.debugN(
        "Sessions inUse {}. Sessions available {}. Uses since last stats: {}. Ids {}. Request queue size {}",
        inUseSessions.size,
        availableSessions.size,
        uses,
        inUseSessions.keys,
        requestQueue.size
      )
      uses = 0
      this

    case Shutdown(done) =>
      timers.cancel(KeepAlive)
      timers.cancel(Stats)
      timers.startSingleTimer(ShutdownTimeout, settings.sessionPool.shutdownTimeout)
      SessionPool.shuttingDown(done, client, availableSessions.toList.map(_.session), inUseSessions)

    case other =>
      log.error("Unexpected message in active state [{}]", other.getClass)
      this
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      cleanupOldSessions(client, availableSessions.map(_.session).toList ++ inUseSessions.values)
      Behaviors.same
    case PreRestart =>
      cleanupOldSessions(client, availableSessions.map(_.session).toList ++ inUseSessions.values)
      Behaviors.same
  }

  private def handOutSessionToQueuedRequest(): Unit =
    if (requestQueue.nonEmpty) {
      val getSession = requestQueue.dequeue()
      handOutSession(getSession.id, getSession.replyTo)
    }

  private def handOutSession(sessionId: Long, requestor: ActorRef[PooledSession]): Unit = {
    val next = availableSessions.dequeue()
    log.traceN("Handing out session [{}], [{}], to [{}]", sessionId, next.session.name, requestor)
    requestor ! PooledSession(next.session, sessionId)
    inUseSessions += (sessionId -> next.session)
  }
}
