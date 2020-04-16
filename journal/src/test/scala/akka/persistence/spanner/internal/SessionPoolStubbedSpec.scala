/*
 * Copyright (C) 2018-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.util.UUID

import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import akka.persistence.spanner.SpannerSettings
import akka.persistence.spanner.internal.SessionPool.{GetSession, PoolBusy, PooledSession, Response}
import akka.persistence.spanner.internal.SessionPoolStubbedSpec.{BatchSessionCreateInvocation, StubbedSpannerClient}
import com.google.spanner.v1.{BatchCreateSessionsRequest, BatchCreateSessionsResponse, Session}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object SessionPoolStubbedSpec {
  case class BatchSessionCreateInvocation(
      input: BatchCreateSessionsRequest,
      response: Promise[BatchCreateSessionsResponse]
  )
  class StubbedSpannerClient(val probe: TestProbe[BatchSessionCreateInvocation]) extends AbstractStubbedSpannerClient {
    override def batchCreateSessions(in: BatchCreateSessionsRequest): Future[BatchCreateSessionsResponse] = {
      val promise = Promise[BatchCreateSessionsResponse]
      probe.ref ! BatchSessionCreateInvocation(in, promise)
      promise.future
    }
  }
}

class SessionPoolStubbedSpec extends AnyWordSpecLike with BeforeAndAfterAll with Matchers {
  val testkit = ActorTestKit()

  val baseSettings = new SpannerSettings(ConfigFactory.parseString("""
         session-pool {
           max-size = 5
           retry-create-interval = 300ms
           max-outstanding-requests = 1
         }
      """).withFallback(ConfigFactory.load().getConfig("akka.persistence.spanner")))

  class Setup() {
    val probe = testkit.createTestProbe[BatchSessionCreateInvocation]()
    val stub = new StubbedSpannerClient(probe)
    val pool = testkit.spawn(SessionPool(stub, baseSettings))
    val sessions = List(Session("s1"), Session("s2"))
    val sessionProbe = testkit.createTestProbe[Response]()
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val id4 = UUID.randomUUID()
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
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    testkit.shutdownTestKit()
  }
}
