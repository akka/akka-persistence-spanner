package akka.persistence.spanner

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.spanner.ActiveActiveSpec.MyActiveActiveStringSet
import akka.persistence.spanner.javadsl.SpannerReadJournal
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.scaladsl.ActiveActiveEventSourcing
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior

import scala.concurrent.duration._

object ActiveActiveSpec {
  object MyActiveActiveStringSet {
    trait Command extends CborSerializable
    case class Add(text: String, replyTo: ActorRef[Done]) extends Command
    case class GetTexts(replyTo: ActorRef[Texts]) extends Command
    case object Stop extends Command
    case class Texts(texts: Set[String]) extends CborSerializable

    def apply(entityId: String, replicaId: ReplicaId, allReplicas: Set[ReplicaId]): Behavior[Command] =
      ActiveActiveEventSourcing.withSharedJournal(entityId, replicaId, allReplicas, SpannerReadJournal.Identifier) {
        aaContext =>
          EventSourcedBehavior[Command, String, Set[String]](
            aaContext.persistenceId,
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

class ActiveActiveSpec extends SpannerSpec {
  override def withMetadata = true
  override def withSnapshotStore = true

  "Active active" must {
    "work with Spanner as journal" in {
      val replicaIdA = ReplicaId("DC-A")
      val replicaIdB = ReplicaId("DC-B")
      val allReplicas = Set(replicaIdA, replicaIdB)
      val aBehavior = MyActiveActiveStringSet("id-1", replicaIdA, allReplicas)
      val replicaA = testKit.spawn(aBehavior)
      val replicaB = testKit.spawn(MyActiveActiveStringSet("id-1", replicaIdB, allReplicas))

      val doneProbe = testKit.createTestProbe[Done]()
      replicaA ! MyActiveActiveStringSet.Add("added to a", doneProbe.ref)
      replicaB ! MyActiveActiveStringSet.Add("added to b", doneProbe.ref)
      doneProbe.receiveMessages(2)

      val stopProbe = testKit.createTestProbe()
      replicaA ! MyActiveActiveStringSet.Stop
      stopProbe.expectTerminated(replicaA)

      val restartedReplicaA = testKit.spawn(aBehavior)
      eventually(interval(200.millis)) {
        // FIXME this fails never seeing the replicated event, have not been able to figure out why
        val probe = testKit.createTestProbe[MyActiveActiveStringSet.Texts]()
        restartedReplicaA ! MyActiveActiveStringSet.GetTexts(probe.ref)
        probe.receiveMessage().texts should ===(Set("added to a", "added to b"))
      }
    }
  }
}
