/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.NotUsed

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import akka.actor.typed.ActorRef
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool.PooledSession
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.google.protobuf.empty.Empty
import com.google.protobuf.struct.Struct
import com.google.spanner.v1._
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future

class StubbedSpannerGrpcClientSpec
    extends ScalaTestWithActorTestKit
    with Matchers
    with AnyWordSpecLike
    with LogCapturing {
  val settings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))

  "The SpannerGrpcClient" must {
    "retry writes if grpc status is ABORTED" in {
      val retries = new AtomicInteger(3)
      val fakeClient = new AbstractStubbedSpannerClient {
        override def batchCreateSessions(in: BatchCreateSessionsRequest): Future[BatchCreateSessionsResponse] =
          Future.successful(
            BatchCreateSessionsResponse(
              Seq(Session("fake session"))
            )
          )

        override def getSession(in: GetSessionRequest): Future[Session] =
          Future.successful(Session("fake session"))

        override def deleteSession(in: DeleteSessionRequest): Future[Empty] = Future.successful(Empty())

        override def commit(in: CommitRequest): Future[CommitResponse] = {
          val count = retries.decrementAndGet()
          if (count > 0) Future.failed(new StatusRuntimeException(Status.ABORTED))
          else Future.successful(new CommitResponse())
        }
      }
      val client = new SpannerGrpcClient(
        "retry-write",
        fakeClient,
        system,
        settings
      );

      // should not fail
      client.withSession(session => client.write(Seq(Mutation()))(session)).futureValue
      // should have retried
      retries.get should ===(0)
    }

    "retry batch updates if grpc status is ABORTED" in {
      val retries = new AtomicInteger(3)
      val fakeClient = new AbstractStubbedSpannerClient {
        override def batchCreateSessions(in: BatchCreateSessionsRequest): Future[BatchCreateSessionsResponse] =
          Future.successful(
            BatchCreateSessionsResponse(
              Seq(Session("fake session"))
            )
          )

        override def getSession(in: GetSessionRequest): Future[Session] =
          Future.successful(Session("fake session"))

        override def deleteSession(in: DeleteSessionRequest): Future[Empty] = Future.successful(Empty())

        override def beginTransaction(in: BeginTransactionRequest): Future[Transaction] =
          Future.successful(Transaction())

        override def executeBatchDml(in: ExecuteBatchDmlRequest): Future[ExecuteBatchDmlResponse] =
          Future.successful(ExecuteBatchDmlResponse())

        override def commit(in: CommitRequest): Future[CommitResponse] = {
          // Note: I expect the failure will happen here, but I guess it can happen in beginTransaction or executeBatchDml as well.
          val count = retries.decrementAndGet()
          if (count > 0) Future.failed(new StatusRuntimeException(Status.ABORTED))
          else Future.successful(new CommitResponse())
        }
      }
      val client = new SpannerGrpcClient(
        "retry-batch-dml",
        fakeClient,
        system,
        settings
      );

      // should not fail
      client
        .withSession(
          session =>
            client
              .executeBatchDml(
                List(
                  ("PRETENDING TO BE A DML QUERY", Struct(), Map.empty),
                  ("PRETENDING TO BE ANOTHER DML QUERY", Struct(), Map.empty)
                )
              )(session)
        )
        .futureValue
      // should have retried
      retries.get should ===(0)
    }

    "request session to re-recreated if session NOT_FOUND" in {
      val pool = createTestProbe[SessionPool.Command]
      val fakeClient = new AbstractStubbedSpannerClient {
        override def executeSql(in: ExecuteSqlRequest): Future[ResultSet] =
          Future.failed(
            Status.fromCode(Status.Code.NOT_FOUND).withDescription("Session not found").asRuntimeException()
          )
      }
      val client = new SpannerGrpcClient("session-re-create", fakeClient, system, settings) {
        override protected def createPool(): ActorRef[SessionPool.Command] = pool.ref
      }
      client.withSession(session => client.executeQuery("select * from cats", Struct(), Map.empty)(session))
      val request = pool.expectMessageType[SessionPool.GetSession]
      request.replyTo ! PooledSession(Session("cat"), request.id)
      pool.expectMessage(SessionPool.ReleaseSession(request.id, recreate = true))
    }

    "request session to re-recreated if session NOT_FOUND during streaming query" in {
      val pool = createTestProbe[SessionPool.Command]
      val fakeClient = new AbstractStubbedSpannerClient {
        override def executeStreamingSql(
            in: com.google.spanner.v1.ExecuteSqlRequest
        ): Source[com.google.spanner.v1.PartialResultSet, akka.NotUsed] =
          Source.failed[com.google.spanner.v1.PartialResultSet](
            Status.fromCode(Status.Code.NOT_FOUND).withDescription("Session not found").asRuntimeException()
          )
      }
      val client = new SpannerGrpcClient("session-re-create", fakeClient, system, settings) {
        override protected def createPool(): ActorRef[SessionPool.Command] = pool.ref
      }
      client.streamingQuery("select * from cats", None, Map.empty).runWith(Sink.ignore)
      val request = pool.expectMessageType[SessionPool.GetSession]
      request.replyTo ! PooledSession(Session("cat"), request.id)
      pool.expectMessage(SessionPool.ReleaseSession(request.id, recreate = true))
    }
  }
}
