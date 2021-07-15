/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit, TestProbe}
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool._
import akka.persistence.spanner.internal.SessionPoolStubbedSpec._
import com.google.protobuf.empty.Empty
import com.google.spanner.v1._
import com.typesafe.config.ConfigFactory
import io.grpc.{Status, StatusRuntimeException}
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
  case class CreateSessionInvocation(
      input: CreateSessionRequest,
      response: Promise[Session]
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

    override def createSession(in: CreateSessionRequest): Future[Session] = {
      val promise = Promise[Session]
      probe.ref ! CreateSessionInvocation(in, promise)
      promise.future
    }
  }
}

class SessionPoolStubbedSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.parseString("akka.loglevel = DEBUG"))
    with Matchers
    with AnyWordSpecLike
    with LogCapturing {
  class Setup(nrSessions: Int = 2, outstandingRequest: Int = 1) {
    val settings = new SpannerSettings(
      ConfigFactory
        .parseString(s"""
         session-pool {
           max-size = $nrSessions
           retry-create-interval = 500ms
           max-outstanding-requests = $outstandingRequest
           keep-alive-interval = 1s
           shutdown-timeout = 1s
         }
      """)
        .withFallback(ConfigFactory.load().getConfig("akka.persistence.spanner"))
    )
    val probe = testKit.createTestProbe[Invocation]()
    val stub = new StubbedSpannerClient(probe)
    val pool = testKit.spawn(SessionPool(stub, settings))
    val sessions = (0 until nrSessions) map { i =>
      Session(s"s$i")
    }
    val sessionProbe = testKit.createTestProbe[Response]()
    val id1 = 1L
    val id2 = 2L
    val id3 = 3L
    val id4 = 4L

    def expectBatchCreate() = {
      val response = probe.expectMessageType[BatchSessionCreateInvocation].response
      response.complete(Success(BatchCreateSessionsResponse(sessions)))
    }
  }

  "StubbedSessionPool" should {
    "batch create the configured number of sessions" in new Setup {
      val invocation = probe.expectMessageType[BatchSessionCreateInvocation]
      invocation.input shouldEqual BatchCreateSessionsRequest(
        settings.fullyQualifiedDatabase,
        sessionCount = settings.sessionPool.maxSize
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
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectMessageType[PooledSession]
      testKit.stop(pool)
      sessions.map { _ =>
        probe.expectMessageType[DeleteSessionInvocation].input.name
      }.toSet shouldEqual sessions.map(_.name).toSet
    }

    "returns sessions in round robin order" in new Setup {
      expectBatchCreate()
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectMessageType[PooledSession].session shouldEqual sessions(0)
      pool ! ReleaseSession(id1, recreate = false)

      pool ! GetSession(sessionProbe.ref, id2)
      sessionProbe.expectMessageType[PooledSession].session shouldEqual sessions(1)
      pool ! ReleaseSession(id2, recreate = false)

      pool ! GetSession(sessionProbe.ref, id3)
      sessionProbe.expectMessageType[PooledSession].session shouldEqual sessions(0)
      pool ! ReleaseSession(id3, recreate = false)
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

    "not provide a session that is waiting for keep alive to complete" in new Setup {
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

    "return sessions in the event of failure that isn't NOT FOUND during keep alive" in new Setup {
      expectBatchCreate()
      val keepAliveRequest1 = probe.expectMessageType[ExecuteSqlInvocation]

      // the 2 sessions are used up, with max 1 outstanding request
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectNoMessage() // should be stashed

      keepAliveRequest1.response.complete(Failure(new RuntimeException("oh dear") with NoStackTrace))
      // session should still be available
      sessionProbe.expectMessageType[PooledSession].id shouldEqual id1
    }

    // TODO, try this with real spanner, e.g. use a session that doesn't exist and
    // create a session, sleep for 1.5 hours and then try to use it
    "recreate sessions if keep alive returns NOT_FOUND" in new Setup {
      expectBatchCreate()
      val keepAliveRequest1 = probe.expectMessageType[ExecuteSqlInvocation]
      keepAliveRequest1.response.complete(Failure(new StatusRuntimeException(Status.NOT_FOUND)))
      val keepAliveRequest2 = probe.expectMessageType[ExecuteSqlInvocation]
      keepAliveRequest2.response.complete(Success(ResultSet()))

      val recreateSession = probe.expectMessageType[CreateSessionInvocation]
      recreateSession.input.database shouldEqual settings.fullyQualifiedDatabase

      val newSession = Session("cat")
      recreateSession.response.complete(Success(newSession))

      // sessions should be one of the original and the new one
      pool ! GetSession(sessionProbe.ref, id1)
      pool ! GetSession(sessionProbe.ref, id2)
      val responses = Set(sessionProbe.expectMessageType[PooledSession], sessionProbe.expectMessageType[PooledSession])

      responses.map(_.session.name) shouldEqual Set(keepAliveRequest2.input.session, newSession.name)
    }

    "keep retrying session create if it fails" in new Setup(nrSessions = 1) {
      expectBatchCreate()
      val keepAliveRequest = probe.expectMessageType[ExecuteSqlInvocation]
      keepAliveRequest.response.complete(Failure(new StatusRuntimeException(Status.NOT_FOUND)))

      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectNoMessage()

      val recreateSession = probe.expectMessageType[CreateSessionInvocation]
      recreateSession.input.database shouldEqual settings.fullyQualifiedDatabase
      recreateSession.response.failure(new RuntimeException("oh no"))
      sessionProbe.expectNoMessage()

      val recreateSessionAgain = probe.expectMessageType[CreateSessionInvocation]
      recreateSessionAgain.input.database shouldEqual settings.fullyQualifiedDatabase
      recreateSessionAgain.response.failure(new RuntimeException("oh no"))
      sessionProbe.expectNoMessage()

      val newSession = Session("cat")
      val recreateSessionAgain2 = probe.expectMessageType[CreateSessionInvocation]
      recreateSessionAgain2.input.database shouldEqual settings.fullyQualifiedDatabase
      recreateSessionAgain2.response.success(newSession)

      sessionProbe.expectMessageType[PooledSession].session shouldEqual newSession
    }

    "recreate sessions if ReleaseSession says so" in new Setup(nrSessions = 1) {
      expectBatchCreate()
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectMessageType[PooledSession].session shouldEqual sessions(0)
      pool ! ReleaseSession(id1, recreate = true)
      val newSessionRequest = probe.expectMessageType[CreateSessionInvocation]

      // session should not have been returned to the pool
      pool ! GetSession(sessionProbe.ref, id2)
      sessionProbe.expectNoMessage()

      val newSession = Session("cat")
      newSessionRequest.response.complete(Success(newSession))

      sessionProbe.expectMessageType[PooledSession].session shouldEqual newSession
    }
  }

  "Shutting down" should {
    "reject requests" in new Setup {
      expectBatchCreate()
      pool ! Shutdown(Promise[Done])
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectMessage(PoolShuttingDown(id1))
    }

    "shutdown once all sessions have been returned" in new Setup {
      expectBatchCreate()
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectMessageType[PooledSession]
      pool ! Shutdown(Promise[Done])
      // pool should not shutdown yet
      pool ! GetSession(sessionProbe.ref, id2)
      sessionProbe.expectMessage(PoolShuttingDown(id2))
      pool ! ReleaseSession(id1, recreate = false)
      sessions.map { _ =>
        probe.expectMessageType[DeleteSessionInvocation].input.name
      }.toSet shouldEqual sessions.map(_.name).toSet
      probe.expectTerminated(pool)
    }

    "timeout and delete sessions" in new Setup(nrSessions = 2) {
      expectBatchCreate()
      pool ! GetSession(sessionProbe.ref, id1)
      sessionProbe.expectMessageType[PooledSession]
      // don't give the session back
      pool ! Shutdown(Promise[Done])
      sessions.map { _ =>
        probe.expectMessageType[DeleteSessionInvocation].input.name
      }.toSet shouldEqual sessions.map(_.name).toSet

      probe.expectTerminated(pool)
    }
  }
}
