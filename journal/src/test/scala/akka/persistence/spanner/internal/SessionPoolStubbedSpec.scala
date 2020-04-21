/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.util.UUID

import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool.{GetSession, PoolBusy, PooledSession, ReleaseSession, Response}
import akka.persistence.spanner.internal.SessionPoolStubbedSpec.{
  BatchSessionCreateInvocation,
  DeleteSessionInvocation,
  ExecuteSqlInvocation,
  Invocation,
  StubbedSpannerClient
}
import com.google.protobuf.empty.Empty
import com.google.spanner.v1._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

object SessionPoolStubbedSpec {
  sealed trait Invocation
  case class BatchSessionCreateInvocation(
      input: BatchCreateSessionsRequest,
      response: Promise[BatchCreateSessionsResponse]
  ) extends Invocation
  case class ExecuteSqlInvocation(
      input: ExecuteSqlRequest,
      response: Promise[ResultSet]
  ) extends Invocation

  case class DeleteSessionInvocation(input: DeleteSessionRequest, response: Promise[Empty]) extends Invocation

  class StubbedSpannerClient(val probe: TestProbe[Invocation]) extends AbstractStubbedSpannerClient {
    override def batchCreateSessions(in: BatchCreateSessionsRequest): Future[BatchCreateSessionsResponse] = {
      val promise = Promise[BatchCreateSessionsResponse]
      probe.ref ! BatchSessionCreateInvocation(in, promise)
      promise.future
    }

    override def deleteSession(in: DeleteSessionRequest): Future[Empty] = {
      val promise = Promise[Empty]
      probe.ref ! DeleteSessionInvocation(in, promise)
      promise.future
    }

    override def executeSql(in: ExecuteSqlRequest): Future[ResultSet] = {
      val promise = Promise[ResultSet]
      probe.ref ! ExecuteSqlInvocation(in, promise)
      promise.future
    }
  }
}

class SessionPoolStubbedSpec extends ScalaTestWithActorTestKit with Matchers with AnyWordSpecLike {
  val baseSettings = new SpannerSettings(ConfigFactory.parseString("""
         session-pool {
           max-size = 2
           retry-create-interval = 300ms
           max-outstanding-requests = 1
           keep-alive-interval = 1s
         }
      """).withFallback(ConfigFactory.load().getConfig("akka.persistence.spanner")))

  class Setup() {
    val probe = testKit.createTestProbe[Invocation]()
    val stub = new StubbedSpannerClient(probe)
    val pool = testKit.spawn(SessionPool(stub, baseSettings))
    val sessions = List(Session("s1"), Session("s2"))
    val sessionProbe = testKit.createTestProbe[Response]()
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val id4 = UUID.randomUUID()

    def expectBatchCreate() = {
      val response = probe.expectMessageType[BatchSessionCreateInvocation].response
      response.complete(Success(BatchCreateSessionsResponse(sessions)))
    }
  }

  "StubbedSessionPool" should {
    "batch create the configured number of sessions" in new Setup {
      val invocation = probe.expectMessageType[BatchSessionCreateInvocation]
      invocation.input shouldEqual BatchCreateSessionsRequest(
        baseSettings.fullyQualifiedDatabase,
        sessionCount = baseSettings.sessionPool.maxSize
      )
    }

    "stashes requests until batch session create completes" in new Setup {
      val response = probe.expectMessageType[BatchSessionCreateInvocation].response

      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectNoMessage()

      response.complete(Success(BatchCreateSessionsResponse(sessions)))
      sessionProbe.expectMessage(PooledSession(sessions.head, id1))
    }

    "retries session creation if fails" in new Setup {
      val response = probe.expectMessageType[BatchSessionCreateInvocation].response

      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectNoMessage()

      response.complete(Failure(TestException("oh dear")))
      probe.expectNoMessage(100.millis) // retry interval is 300ms

      // retry - return success
      val responseTake2 = probe.expectMessageType[BatchSessionCreateInvocation].response
      responseTake2.complete(Success(BatchCreateSessionsResponse(sessions)))

      sessionProbe.expectMessage(PooledSession(sessions.head, id1))
    }

    "times out for session creation" in new Setup {
      pending
    }

    "rejects when stash is full when waiting on session create" in new Setup {
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectNoMessage()
      pool ! GetSession(sessionProbe.ref, id2)
      sessionProbe.expectMessage(PoolBusy(id2))
    }

    "rejects when stash is full when in active mode" in new Setup {
      val response = probe.expectMessageType[BatchSessionCreateInvocation].response
      response.complete(Success(BatchCreateSessionsResponse(sessions)))
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectMessageType[PooledSession]
      pool ! GetSession(sessionProbe.ref, id2)
      sessionProbe.expectMessageType[PooledSession]
      // all sessions are used, one can be stashed then pool should return busy
      pool ! GetSession(sessionProbe.ref, id3)
      sessionProbe.expectNoMessage()
      pool ! GetSession(sessionProbe.ref, id4)
      sessionProbe.expectMessage(PoolBusy(id4))
    }

    "deletes sessions on shutdown" in new Setup {
      val response = probe.expectMessageType[BatchSessionCreateInvocation].response
      response.complete(Success(BatchCreateSessionsResponse(sessions)))
      testKit.stop(pool)
      sessions.foreach { session =>
        val request = probe.expectMessageType[DeleteSessionInvocation]
        request.input shouldEqual DeleteSessionRequest(session.name)
      }
    }

    "returns sessions in round robin order" in new Setup {
      expectBatchCreate()
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectMessageType[PooledSession].session shouldEqual sessions(0)
      pool ! ReleaseSession(id1)

      pool ! GetSession(sessionProbe.ref, id2)
      sessionProbe.expectMessageType[PooledSession].session shouldEqual sessions(1)
      pool ! ReleaseSession(id2)

      pool ! GetSession(sessionProbe.ref, id3)
      sessionProbe.expectMessageType[PooledSession].session shouldEqual sessions(0)
      pool ! ReleaseSession(id3)
    }

    "keeps alive un-used sessions" in new Setup {
      expectBatchCreate()
      probe.expectNoMessage(100.millis)
      val keepAliveRequest1 = probe.expectMessageType[ExecuteSqlInvocation]
      val keepAliveRequest2 = probe.expectMessageType[ExecuteSqlInvocation]
      keepAliveRequest1.input.sql shouldEqual "SELECT 1"
      keepAliveRequest2.input.sql shouldEqual "SELECT 1"
      Set(keepAliveRequest1.input.session, keepAliveRequest2.input.session) shouldEqual sessions.map(_.name).toSet
    }

    "sessions should not be available during keep alive" in new Setup {
      expectBatchCreate()
      val keepAliveRequest1 = probe.expectMessageType[ExecuteSqlInvocation]
      val keepAliveRequest2 = probe.expectMessageType[ExecuteSqlInvocation]

      // the 2 sessions are used up, with max 1 outstanding request
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectNoMessage() // should be stashed

      pool ! GetSession(sessionProbe.ref, id2)
      sessionProbe.expectMessageType[PoolBusy].id shouldEqual id2 // eagerly rejected

      // and now the stashed request should get an answer
      keepAliveRequest1.response.complete(Success(ResultSet()))
      keepAliveRequest2.response.complete(Success(ResultSet()))
      sessionProbe.expectMessageType[PooledSession].id shouldEqual id1

      // and another should also work
      pool ! GetSession(sessionProbe.ref, id2)
      sessionProbe.expectMessageType[PooledSession].id shouldEqual id2
    }

    "sessions should be returned in the event of failure during keep alive" in new Setup {
      expectBatchCreate()
      val keepAliveRequest1 = probe.expectMessageType[ExecuteSqlInvocation]
      val keepAliveRequest2 = probe.expectMessageType[ExecuteSqlInvocation]

      // the 2 sessions are used up, with max 1 outstanding request
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectNoMessage() // should be stashed

      keepAliveRequest1.response.complete(Failure(new RuntimeException("oh dear") with NoStackTrace))
      // session should still be available
      sessionProbe.expectMessageType[PooledSession].id shouldEqual id1
    }
  }
}
