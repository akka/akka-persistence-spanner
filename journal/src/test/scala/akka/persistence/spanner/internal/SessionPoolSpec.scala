/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern._
import akka.persistence.spanner.SpannerSpec
import akka.persistence.spanner.internal.SessionPool.{GetSession, PooledSession, ReleaseSession, Response}
import akka.util.Timeout

import scala.concurrent.duration._

class SessionPoolSpec extends SpannerSpec {
  val testkit = ActorTestKit("SessionPoolSpec")
  implicit val scheduler = testkit.scheduler
  implicit val timeout = Timeout(5.seconds)

  class Setup {
    val pool: ActorRef[SessionPool.Command] = testkit.spawn(SessionPool(spannerClient, spannerSettings))
    val probe = testkit.createTestProbe[Response]()
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
  }

  "A session pool" should {
    "return a session" in new Setup {
      val session =
        pool.ask[Response](replyTo => GetSession(replyTo, id1)).futureValue
      session.id shouldEqual id1
      pool.tell(ReleaseSession(id1))
    }

    "should not return session until one available" in new Setup {
      pool ! GetSession(probe.ref, id1)
      probe.expectMessageType[PooledSession].id shouldEqual id1

      pool ! GetSession(probe.ref, id2)
      probe.expectNoMessage()

      pool ! ReleaseSession(id1)
      probe.expectMessageType[PooledSession].id shouldEqual id2
    }

    "handle invalid return of session" in new Setup {
      pool ! GetSession(probe.ref, id1)
      val session = probe.expectMessageType[PooledSession]

      // oopsy
      pool ! ReleaseSession(id2)

      // pool still works and has the same sessions
      pool ! ReleaseSession(id1)
      pool ! GetSession(probe.ref, id2)
      probe.expectMessageType[PooledSession].session shouldEqual session.session
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    testkit.shutdownTestKit()
  }
}
