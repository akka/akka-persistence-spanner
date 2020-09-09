package akka.persistence.spanner

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.spanner.javadsl.SpannerReadJournal
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing

import scala.concurrent.duration._

object ReplicatedEventSourcingSpec {
  object MyReplicatedStringSet {
    trait Command extends CborSerializable
    case class Add(text: String, replyTo: ActorRef[Done]) extends Command
    case class GetTexts(replyTo: ActorRef[Texts]) extends Command
    case object Stop extends Command
    case class Texts(texts: Set[String]) extends CborSerializable

    def apply(entityId: String, replicaId: ReplicaId, allReplicas: Set[ReplicaId]): Behavior[Command] =
      ReplicatedEventSourcing.commonJournalConfig(
        ReplicationId("type", entityId, replicaId),
        allReplicas,
        SpannerReadJournal.Identifier
      ) { resContext =>
        EventSourcedBehavior[Command, String, Set[String]](
          resContext.persistenceId,
          Set.empty[String],
          (state, command) =>
            command match {
              case Add(text, replyTo) =>
                Effect.persist(text).thenRun(_ => replyTo ! Done)
              case GetTexts(replyTo) =>
                replyTo ! Texts(state)
                Effect.none
              case Stop =>
                Effect.stop()
            },
          (state, event) => state + event
        ).withJournalPluginId("akka.persistence.spanner.journal")
      }
  }
}

class ReplicatedEventSourcingSpec extends SpannerSpec {
  import ReplicatedEventSourcingSpec._

  override def withMetadata = true
  override def withSnapshotStore = true

  "Replicated Event Sourcing" must {
    "work with Spanner as journal" in {
      val replicaIdA = ReplicaId("DC-A")
      val replicaIdB = ReplicaId("DC-B")
      val allReplicas = Set(replicaIdA, replicaIdB)
      val aBehavior = MyReplicatedStringSet("id-1", replicaIdA, allReplicas)
      val replicaA = testKit.spawn(aBehavior)
      val replicaB = testKit.spawn(MyReplicatedStringSet("id-1", replicaIdB, allReplicas))

      val doneProbe = testKit.createTestProbe[Done]()
      replicaA ! MyReplicatedStringSet.Add("added to a", doneProbe.ref)
      replicaB ! MyReplicatedStringSet.Add("added to b", doneProbe.ref)
      doneProbe.receiveMessages(2)

      val stopProbe = testKit.createTestProbe()
      replicaA ! MyReplicatedStringSet.Stop
      stopProbe.expectTerminated(replicaA)

      val restartedReplicaA = testKit.spawn(aBehavior)
      eventually(interval(200.millis)) {
        // FIXME this fails never seeing the replicated event, have not been able to figure out why
        val probe = testKit.createTestProbe[MyReplicatedStringSet.Texts]()
        restartedReplicaA ! MyReplicatedStringSet.GetTexts(probe.ref)
        probe.receiveMessage().texts should ===(Set("added to a", "added to b"))
      }
    }
  }
}
