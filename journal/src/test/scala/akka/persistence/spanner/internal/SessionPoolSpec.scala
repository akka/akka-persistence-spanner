/*
 * Copyright (C) 2021 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

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
  val settings = spannerSettings

  class Setup {
    val pool: ActorRef[SessionPool.Command] = testkit.spawn(SessionPool(spannerClient, spannerSettings))
    val probe = testkit.createTestProbe[Response]()
    val id1 = 1L
    val id2 = 2L
  }

  "A session pool" should {
    "return a session" in new Setup {
      val session =
        pool.ask[Response](replyTo => GetSession(replyTo, id1)).futureValue
      session.id shouldEqual id1
      pool.tell(ReleaseSession(id1, recreate = false))
    }

    "should not return session until one available" in new Setup {
      // take all the sessions
      val ids = (1 to settings.sessionPool.maxSize) map { n =>
        pool ! GetSession(probe.ref, n)
        probe.expectMessageType[PooledSession].id shouldEqual n
        n
      }

      // should be stashed
      pool ! GetSession(probe.ref, 2L)
      probe.expectNoMessage()

      pool ! ReleaseSession(1L, recreate = false)
      probe.expectMessageType[PooledSession].id should ===(2L)
    }

    "handle invalid return of session" in new Setup {
      pool ! GetSession(probe.ref, id1)
      probe.expectMessageType[PooledSession]

      // oopsy
      pool ! ReleaseSession(id2, recreate = false)

      // pool still works
      pool ! ReleaseSession(id1, recreate = false)
      pool ! GetSession(probe.ref, id2)
      probe.expectMessageType[PooledSession]
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    testkit.shutdownTestKit()
  }
}
