/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.util.concurrent.atomic.AtomicLong

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, ActorSystem, SupervisorStrategy}
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool._
import akka.stream.scaladsl.Source
import akka.util.{ConstantFun, Timeout}
import akka.{Done, NotUsed}
import com.google.protobuf.ByteString
import com.google.protobuf.struct.{Struct, Value}
import com.google.rpc.Code
import com.google.spanner.v1.CommitRequest.Transaction
import com.google.spanner.v1._
import io.grpc.StatusRuntimeException
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise, TimeoutException}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object SpannerGrpcClient {
  final class TransactionFailed(code: Int, message: String, details: Any)
      extends RuntimeException(s"Code $code. Message: $message. Params: $details")

  final class PoolBusyException extends RuntimeException("Session pool busy") with NoStackTrace
  final class PoolShuttingException extends RuntimeException("Session pool shutting down") with NoStackTrace

  val PoolBusyException = new PoolBusyException
  val PoolShuttingDownException = new PoolShuttingException

  private val sessionIdCounter = new AtomicLong(0)

  private def nextSessionId(): Long = sessionIdCounter.incrementAndGet()
}

/**
 * A thin wrapper around the gRPC client to expose only what the plugin needs.
 */
@InternalApi private[spanner] class SpannerGrpcClient(
    name: String,
    val client: SpannerClient,
    system: ActorSystem[_],
    settings: SpannerSettings
) {
  import SpannerGrpcClient._

  private implicit val _system = system
  private implicit val ec = system.executionContext

  private val log = LoggerFactory.getLogger(classOf[SpannerGrpcClient])

  private val pool = createPool()

  /**
   * Note that this grabs its own session from the pool rather than require one as a parameter, it will also return that
   * when the query has completed.
   */
  def streamingQuery(
      sql: String,
      params: Option[Struct] = None,
      paramTypes: Map[String, Type] = Map.empty
  ): Source[Seq[Value], Future[Done]] = {
    val sessionId = nextSessionId()
    val result: Future[Source[Seq[Value], NotUsed]] = getSession(sessionId).map { session =>
      if (log.isTraceEnabled()) {
        log.traceN("streamingQuery, session id [{}], query: [{}], params: [{}]", session.id, sql, params)
      }
      client
        .executeStreamingSql(
          ExecuteSqlRequest(session.session.name, sql = sql, params = params, paramTypes = paramTypes)
        )
        .via(RowCollector)
    }
    Source
      .futureSource(result)
      .watchTermination() { (_, terminationFuture) =>
        releaseSessionOnComplete(terminationFuture, sessionId)
      }
  }

  /**
   * Executes a write with retries if result is ABORTED
   */
  def write(mutations: Seq[Mutation])(
      implicit session: PooledSession
  ): Future[Unit] =
    withWriteRetries { () =>
      client
        .commit(
          CommitRequest(
            session.session.name,
            Transaction.SingleUseTransaction(
              TransactionOptions(TransactionOptions.Mode.ReadWrite(TransactionOptions.ReadWrite()))
            ),
            mutations
          )
        )
    }.map(ConstantFun.scalaAnyToUnit)(ExecutionContexts.parasitic)

  /**
   * Executes all the statements in a single BatchDML statement. If query is failed with ABORTED it is retried.
   *
   * @param statements to execute along with their params and param types
   * @return Future is completed with failure if status.code != Code.OK. In that case
   *         the transaction won't be committed and none of the modifications will have
   *         happened.
   */
  def executeBatchDml(statements: List[(String, Struct, Map[String, Type])])(
      implicit session: PooledSession
  ): Future[Unit] =
    withWriteRetries { () =>
      def createBatchDmlRequest(sessionId: String, transactionId: ByteString): ExecuteBatchDmlRequest = {
        val s = statements.map {
          case (sql, params, types) =>
            ExecuteBatchDmlRequest.Statement(
              sql,
              Some(params),
              types
            )
        }
        ExecuteBatchDmlRequest(
          sessionId,
          transaction = Some(TransactionSelector(TransactionSelector.Selector.Id(transactionId))),
          s
        )
      }

      for {
        transaction <- client.beginTransaction(
          BeginTransactionRequest(
            session.session.name,
            Some(TransactionOptions(TransactionOptions.Mode.ReadWrite(TransactionOptions.ReadWrite())))
          )
        )
        resultSet <- client.executeBatchDml(createBatchDmlRequest(session.session.name, transaction.id))
        _ = {
          resultSet.status match {
            case Some(status) if status.code != Code.OK.index =>
              log.warn("Transaction failed with status {}", resultSet.status)
              throw new TransactionFailed(status.code, status.message, status.details)
            case status =>
              if (log.isTraceEnabled())
                log.traceN(
                  "executeBatchDml, session: [{}] status: [{}], resultSets: [{}]",
                  session.id,
                  status,
                  resultSet.resultSets
                )
              resultSet
          }
        }
        commitResponse <- client.commit(
          CommitRequest(session.session.name, CommitRequest.Transaction.TransactionId(transaction.id))
        )
      } yield {
        log.trace("Successful commit at {}", commitResponse.commitTimestamp.map(t => (t.seconds, t.nanos)))
        ()
      }
    }

  /**
   * Execute a small query. Result can not be larger than 10 MiB. Larger results
   * should use `executeStreamingSql`
   *
   * Uses a single use read only transaction.
   */
  def executeQuery(sql: String, params: Struct, paramTypes: Map[String, Type])(
      implicit session: PooledSession
  ): Future[ResultSet] =
    client.executeSql(
      ExecuteSqlRequest(
        session = session.session.name,
        sql = sql,
        params = Some(params),
        paramTypes = paramTypes
      )
    )

  /**
   * Execute the given function with a session.
   */
  def withSession[T](f: PooledSession => Future[T]): Future[T] = {
    val sessionId = nextSessionId()
    val result = getSession(sessionId)
      .flatMap(f)
    releaseSessionOnComplete(result, sessionId)
  }

  /**
   * So far only done on ActorSystem shutdown.
   */
  def shutdown(): Future[Done] = {
    val done = Promise[Done]
    pool ! Shutdown(done)
    done.future
  }

  private def releaseSessionOnComplete[T](f: Future[T], sessionId: Long): Future[T] = {
    f.onComplete {
      case Success(_) =>
        log.trace("Operation successful, releasing session [{}]", sessionId)
        pool.tell(ReleaseSession(sessionId, recreate = false))
      case Failure(PoolBusyException) =>
        // no need to release it
        log.debug("Acquiring session, pool busy")
      case Failure(t: TimeoutException) =>
        // FIXME, why? The request could be in the stash of the mailbox
        // no need to release it
        log.debug("Acquiring session timed out. Session id: " + sessionId, t.getMessage)
      case Failure(t: StatusRuntimeException) if t.getStatus.getCode == SessionDeleted.statusCode =>
        log.warn("User session failed with possible session not found. Re-creating session. Message: {}", t.getMessage)
        pool.tell(ReleaseSession(sessionId, recreate = true))
      case Failure(t) =>
        // release
        log.debug("User query failed: {}. Returning session [{}]", t.getMessage, sessionId)
        pool.tell(ReleaseSession(sessionId, recreate = false))
    }(ExecutionContexts.parasitic)
    f
  }

  private def withWriteRetries[T](f: () => Future[T])(
      implicit session: PooledSession
  ): Future[T] = {
    val deadLine = settings.maxWriteRetryTimeout.fromNow
    def tryWrite(retriesLeft: Int): Future[T] =
      f().recoverWith {
        case ex: StatusRuntimeException
            if ex.getStatus == io.grpc.Status.ABORTED && retriesLeft > 0 && deadLine.hasTimeLeft() =>
          log.debug("Write failed for [{}], retrying", session.id)
          tryWrite(retriesLeft - 1)
      }

    tryWrite(settings.maxWriteRetries)
  }

  protected def getSession(sessionUuid: Long): Future[PooledSession] = {
    implicit val timeout = Timeout(settings.sessionAcquisitionTimeout)
    pool
      .ask[SessionPool.Response](replyTo => GetSession(replyTo, sessionUuid))
      .transform {
        case Success(pt: PooledSession) =>
          log.trace("Acquired session [{}]", pt.id)
          Success(pt)
        case Success(PoolBusy(_)) => Failure(PoolBusyException)
        case Success(PoolShuttingDown(_)) => Failure(PoolShuttingDownException)
        case Failure(t) => Failure(t)
      }(ExecutionContexts.parasitic)
  }

  // exposed for testing
  protected def createPool(): ActorRef[Command] =
    system.systemActorOf(
      Behaviors
        .supervise(SessionPool.apply(client, settings))
        .onFailure(
          SupervisorStrategy.restartWithBackoff(
            settings.sessionPool.restartMinBackoff,
            settings.sessionPool.restartMaxBackoff,
            0.1
          )
        ),
      s"$name-session-pool"
    )
}
