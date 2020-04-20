/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.util.UUID

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool._
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.google.protobuf.struct.Value.Kind.StringValue
import com.google.protobuf.struct.{Struct, Value}
import com.google.rpc.Code
import com.google.spanner.v1.CommitRequest.Transaction
import com.google.spanner.v1._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * INTERNAL API
 */
@InternalApi
private[spanner] object SpannerGrpcClient {
  val DeleteStatementTypes = Map(
    "persistence_id" -> Type(TypeCode.STRING),
    "sequence_nr" -> Type(TypeCode.INT64)
  )

  final class DeleteFailedException(persistenceId: String, code: Int, message: String, details: Any)
      extends RuntimeException(s"Delete for persistence id failed. Code $code. Message: $message. Params: $details")
}

/**
 * A thin wrapper around the gRPC client to expose only what the plugin needs
 *
 * TODO handle all status codes https://github.com/akka/akka-persistence-spanner/issues/14
 */
@InternalApi private[spanner] final class SpannerGrpcClient(
    client: SpannerClient,
    system: ActorSystem[_],
    pool: ActorRef[SessionPool.Command],
    settings: SpannerSettings
) {
  import SpannerGrpcClient._

  private implicit val _system = system
  private implicit val ec = system.executionContext
  // TODO config
  private implicit val timeout = Timeout(5.seconds)

  private val log = Logging(system.toClassic, classOf[SpannerGrpcClient])
  private val SqlDeleteInsertToDeletions =
    s"INSERT INTO ${settings.deletionsTable}(persistence_id, deleted_to) VALUES (@persistence_id, @sequence_nr)"
  private val SqlDelete =
    s"DELETE FROM ${settings.table} where persistence_id = @persistence_id AND sequence_nr <= @sequence_nr"

  def streamingQuery(
      sql: String,
      params: Struct,
      paramTypes: Map[String, Type]
  ): Source[Seq[Value], Future[NotUsed]] = {
    val sessionId = UUID.randomUUID()
    val result = getSession(sessionId).map { session =>
      client
        .executeStreamingSql(
          ExecuteSqlRequest(session.session.name, sql = sql, params = Some(params), paramTypes = paramTypes)
        )
        .mapMaterializedValue(f => f)
        .statefulMapConcat { () =>
          {
            var previousPartialRow: Seq[Value] = Nil
            var columns: Seq[StructType.Field] = null
            partialResultSet => {
              // it is the first row, will contain metadata for columns
              if (columns == null) {
                columns = partialResultSet.metadata.get.rowType.get.fields
              }
              log.debug("result set {}", partialResultSet)
              assert(columns != null, "received a row without first receiving metadata")
              val newValues = previousPartialRow ++ partialResultSet.values
              // TODO handle chunked values https://github.com/akka/akka-persistence-spanner/issues/11
              if (newValues.size >= columns.size) {
                val grouped = newValues.grouped(columns.size).toList
                if (grouped.last.size != columns.size) {
                  previousPartialRow = grouped.last
                } else {
                  previousPartialRow = Nil
                }
                grouped.takeWhile(_.size == columns.size)
              } else {
                previousPartialRow = newValues
                List.empty[Seq[Value]]
              }
            }
          }
        }
    }
    Source
      .futureSource(result)
      .mapMaterializedValue(f => {
        f.onComplete(_ => {
          // TODO double check this is after the Source has completed, not when the Future containing the source completes
          pool.tell(ReleaseSession(sessionId))
        })
        f
      })
  }

  def write(mutations: Seq[Mutation]): Future[Unit] = {
    val sessionUuid = UUID.randomUUID()
    val write = for {
      session <- getSession(sessionUuid)
      _ <- client.commit(
        CommitRequest(
          session.session.name,
          Transaction.SingleUseTransaction(
            TransactionOptions(TransactionOptions.Mode.ReadWrite(TransactionOptions.ReadWrite()))
          ),
          mutations
        )
      )
    } yield ()
    // TODO don't do this if we got pool is busy
    write.onComplete(_ => pool.tell(ReleaseSession(sessionUuid)))
    write
  }

  def executeDelete(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    // TODO on timeout clean up session request
    log.info("executing delete for [{}] to [{}]", persistenceId, toSequenceNr)
    val params = Some(
      Struct(
        Map(
          "persistence_id" -> Value(StringValue(persistenceId)),
          "sequence_nr" -> Value(StringValue(toSequenceNr.toString))
        )
      )
    )
    val sessionUuid = UUID.randomUUID()
    val query = for {
      session <- getSession(sessionUuid)
      transaction <- client.beginTransaction(
        BeginTransactionRequest(
          session.session.name,
          Some(TransactionOptions(TransactionOptions.Mode.ReadWrite(TransactionOptions.ReadWrite())))
        )
      )
      resultSet <- client.executeBatchDml(
        ExecuteBatchDmlRequest(
          session.session.name,
          transaction = Some(TransactionSelector(TransactionSelector.Selector.Id(transaction.id))),
          statements = List(
            ExecuteBatchDmlRequest.Statement(
              SqlDelete,
              params,
              SpannerGrpcClient.DeleteStatementTypes
            ),
            ExecuteBatchDmlRequest.Statement(
              SqlDeleteInsertToDeletions,
              params,
              SpannerGrpcClient.DeleteStatementTypes
            )
          )
        )
      )
      status = {
        resultSet.status match {
          case Some(status) if status.code != Code.OK.index =>
            log.warning("Delete failed with status {}", resultSet.status)
            Future.failed(new DeleteFailedException(persistenceId, status.code, status.message, status.details))
          case _ => Future.successful(())
        }
      }
      _ <- client.commit(CommitRequest(session.session.name, CommitRequest.Transaction.TransactionId(transaction.id)))
    } yield {
      status
    }

    // TODO don't do this if we got pool is busy
    query.onComplete(_ => pool.tell(ReleaseSession(sessionUuid)))
    query.map(_ => ())
  }

  def executeQuery(sql: String, params: Struct, paramTypes: Map[String, Type]): Future[ResultSet] = {
    // TODO on timeout clean up session request
    val sessionUuid = UUID.randomUUID()
    val query = for {
      session <- getSession(sessionUuid)
      resultSet <- client.executeSql(
        ExecuteSqlRequest(
          session = session.session.name,
          sql = sql,
          params = Some(params),
          paramTypes = paramTypes
        )
      )
    } yield {
      resultSet
    }
    // TODO don't do this if we got pool is busy
    query.onComplete(_ => pool.tell(ReleaseSession(sessionUuid)))
    query
  }

  private def getSession(sessionUuid: UUID): Future[PooledSession] =
    pool.ask[SessionPool.Response](replyTo => GetSession(replyTo, sessionUuid)).transform {
      case Success(pt: PooledSession) => Success(pt)
      // TODO specific exception so we can avoid releasing the session
      case Success(PoolBusy(_)) => Failure(new RuntimeException("Unable to execute spanner query. Pool busy."))
      case Failure(t) => Failure(t)
    }
}
