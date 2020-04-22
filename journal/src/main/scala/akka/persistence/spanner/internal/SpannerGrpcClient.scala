/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.util.UUID

import akka.Done
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorSystem, SupervisorStrategy}
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool._
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.google.protobuf.ByteString
import com.google.protobuf.struct.{Struct, Value}
import com.google.rpc.Code
import com.google.spanner.v1.CommitRequest.Transaction
import com.google.spanner.v1._

import scala.concurrent.Future
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object SpannerGrpcClient {
  final class TransactionFailed(code: Int, message: String, details: Any)
      extends RuntimeException(s"Code $code. Message: $message. Params: $details")

  final class PoolBusyException extends RuntimeException with NoStackTrace

  val PoolBusyException = new PoolBusyException
}

/**
 * A thin wrapper around the gRPC client to expose only what the plugin needs.
 */
@InternalApi private[spanner] final class SpannerGrpcClient(
    name: String,
    client: SpannerClient,
    system: ActorSystem[_],
    settings: SpannerSettings
) {
  import SpannerGrpcClient._

  private implicit val _system = system
  private implicit val ec = system.executionContext

  private val log = Logging(system.toClassic, classOf[SpannerGrpcClient])

  private val pool = system.systemActorOf(
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

  def streamingQuery(
      sql: String,
      params: Struct,
      paramTypes: Map[String, Type]
  ): Source[Seq[Value], Future[Done]] = {
    val sessionId = UUID.randomUUID()
    val result = getSession(sessionId).map { session =>
      client
        .executeStreamingSql(
          ExecuteSqlRequest(session.session.name, sql = sql, params = Some(params), paramTypes = paramTypes)
        )
        .via(RowCollector)
    }
    Source
      .futureSource(result)
      .watchTermination() { (_, terminationFuture) =>
        terminationFuture.onComplete { _ =>
          pool.tell(ReleaseSession(sessionId))
        }
        terminationFuture
      }
  }

  /**
   * This doesn't do retries. See
   * https://github.com/akka/akka-persistence-spanner/issues/18 for re-trying
   * with the same session
   */
  def write(mutations: Seq[Mutation]): Future[Unit] =
    withSession { session =>
      client.commit(
        CommitRequest(
          session.session.name,
          Transaction.SingleUseTransaction(
            TransactionOptions(TransactionOptions.Mode.ReadWrite(TransactionOptions.ReadWrite()))
          ),
          mutations
        )
      )
    }.map(_ => ())

  /**
   * Executes all the statements in a single BatchDML statement.
   *
   * @param statements to execute along with their params and param types
   * @return Future is completed with faiure if status.code != Code.OK. In that case
   *         the transaction won't be commited and none of the modifications will have
   *         happened.
   */
  def executeBatchDml(statements: List[(String, Struct, Map[String, Type])]): Future[Unit] = {
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

    withSession { session =>
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
              log.warning("Transaction failed with status {}", resultSet.status)
              Future.failed(new TransactionFailed(status.code, status.message, status.details))
            case _ => Future.successful(())
          }
        }
        _ <- client.commit(
          CommitRequest(session.session.name, CommitRequest.Transaction.TransactionId(transaction.id))
        )
      } yield ()
    }
  }

  /**
   * Execute a small query. Result can not be larger than 10 MiB. Larger results
   * should use `executeStreamingSql`
   *
   * Uses a single use read only transaction.
   */
  def executeQuery(sql: String, params: Struct, paramTypes: Map[String, Type]): Future[ResultSet] =
    withSession(
      session =>
        client.executeSql(
          ExecuteSqlRequest(
            session = session.session.name,
            sql = sql,
            params = Some(params),
            paramTypes = paramTypes
          )
        )
    )

  /**
   * Execute the given function with a session.
   */
  private def withSession[T](f: PooledSession => Future[T]): Future[T] = {
    val sessionUuid = UUID.randomUUID()
    val result = getSession(sessionUuid).flatMap(f)
    result.onComplete {
      case Success(_) =>
        pool.tell(ReleaseSession(sessionUuid))
      //release
      case Failure(PoolBusyException) =>
      // no need to release it
      case Failure(_) =>
        pool.tell(ReleaseSession(sessionUuid))
      // release
    }((ExecutionContexts.parasitic))
    result
  }

  private def getSession(sessionUuid: UUID): Future[PooledSession] = {
    implicit val timeout = Timeout(settings.sessionAcquisitionTimeout)
    pool
      .ask[SessionPool.Response](replyTo => GetSession(replyTo, sessionUuid))
      .transform {
        case Success(pt: PooledSession) => Success(pt)
        case Success(PoolBusy(_)) => Failure(PoolBusyException)
        case Failure(t) => Failure(t)
      }(ExecutionContexts.parasitic)
  }
}
