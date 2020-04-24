/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.spanner.SpannerSettings
import com.google.protobuf.empty.Empty
import com.google.protobuf.struct.Struct
import com.google.spanner.v1._
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future

class StubbedSpannerGrpcClientSpec extends ScalaTestWithActorTestKit with Matchers with AnyWordSpecLike {
  "The SpannerGrpcClient" must {
    "retry writes if grpc status is ABORTED" in {
      val retries = new AtomicInteger(3)
      val settings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))
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
      val settings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))
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
  }
}
