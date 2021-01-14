package akka.persistence.spanner

import akka.Done
import akka.persistence.spanner.TestActors.Persister
import akka.persistence.spanner.internal.SpannerGrpcClientExtension
import com.google.spanner.v1.DeleteSessionRequest
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

class SessionFaultToleranceSpec extends SpannerSpec {
  override def customConfig: Config = ConfigFactory.parseString("""
       # single session makes testing easier
       akka.persistence.spanner.session-pool.max-size = 1
      """)

  "Failure handling when the session is invalidated" must {
    "recover from a replay failure" in {
      // create a persistent actor
      val pid = nextPid()
      val persister = testKit.spawn(Persister(pid))
      val probe = testKit.createTestProbe[Done]()
      persister ! Persister.PersistMe("e-1", probe.ref)
      probe.expectMessage(Done)

      // same client the journal will use
      val spannerGrpcClient = SpannerGrpcClientExtension(testKit.system).clientFor("akka.persistence.spanner")

      // sneakily delete the session id without letting the pool know, now it is returned to the pool
      // but spanner has deleted it
      spannerGrpcClient.withSession { session =>
        testKit.system.log.debug("Sneakily deleting session {}", session.session.name)
        spannerGrpcClient.client.deleteSession(DeleteSessionRequest.of(session.session.name))
      }.futureValue

      // replay fails
      val persisterIncarnation2 = testKit.spawn(Persister(pid))
      probe.expectTerminated(persisterIncarnation2)

      // but session should be is invalidated, and a new one created so that the next operation succeeds
      val persisterIncarnation3 = testKit.spawn(Persister(pid))
      persisterIncarnation3 ! Persister.Ping(probe.ref)
      probe.expectMessage(Done)
    }

    // covers bug #99
    "recover from a write failure" in {
      // create a persistent actor
      val pid = nextPid()
      val persister = testKit.spawn(Persister(pid))
      val probe = testKit.createTestProbe[Done]()
      persister ! Persister.PersistMe("e-1", probe.ref)
      probe.expectMessage(Done)

      // same client the journal will use
      val spannerGrpcClient = SpannerGrpcClientExtension(testKit.system).clientFor("akka.persistence.spanner")

      // sneakily delete the session id without letting the pool know, now it is returned to the pool
      // but spanner has deleted it
      spannerGrpcClient.withSession { session =>
        testKit.system.log.debug("Sneakily deleting session {}", session.session.name)
        spannerGrpcClient.client.deleteSession(DeleteSessionRequest.of(session.session.name))
      }.futureValue

      // trigger a write failure, stopping the actor
      persister ! Persister.PersistMe("e-1", probe.ref)
      probe.expectTerminated(persister)

      // session should be invalidated, and a new one created so that the next operation
      // (replay in this case) succeeds
      val persisterIncarnation2 = testKit.spawn(Persister(pid))
      persisterIncarnation2 ! Persister.Ping(probe.ref)
      probe.expectMessage(Done)
    }
  }
}
