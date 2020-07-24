package akka.persistence.spanner

import akka.Done
import akka.actor.testkit.typed.FishingOutcome
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.JournalProtocol.WriteMessages
import akka.persistence.Persistence
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SelectedSnapshot
import akka.persistence.SnapshotMetadata
import akka.persistence.SnapshotProtocol.LoadSnapshot
import akka.persistence.SnapshotProtocol.LoadSnapshotResult
import akka.persistence.SnapshotProtocol.SaveSnapshot
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.spanner.ActiveActiveSpec.MyActiveActiveStringSet
import akka.persistence.spanner.javadsl.SpannerReadJournal
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.internal.ReplicatedSnapshotMetadata
import akka.persistence.typed.scaladsl.ActiveActiveEventSourcing
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior

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
  override def replicatedMetaEnabled = true
  override def withSnapshotStore = true

  "Active active" must {
    "work with Spanner as journal" in {
      val allReplicas = Set(ReplicaId("DC-A"), ReplicaId("DC-B"))
      val aBehavior = MyActiveActiveStringSet("id-1", ReplicaId("DC-A"), allReplicas)
      val replicaA = testKit.spawn(aBehavior)
      val replicaB = testKit.spawn(MyActiveActiveStringSet("id-1", ReplicaId("DC-B"), allReplicas))

      val doneProbe = testKit.createTestProbe[Done]()
      replicaA ! MyActiveActiveStringSet.Add("added to a", doneProbe.ref)
      replicaB ! MyActiveActiveStringSet.Add("added to b", doneProbe.ref)
      doneProbe.receiveMessages(2)

      val stopProbe = testKit.createTestProbe()
      replicaA ! MyActiveActiveStringSet.Stop
      stopProbe.expectTerminated(replicaA)

      val restartedReplicaA = testKit.spawn(aBehavior)
      eventually {
        // FIXME this fails never seeing the replicated event, have not been able to figure out why
        val probe = testKit.createTestProbe[MyActiveActiveStringSet.Texts]()
        restartedReplicaA ! MyActiveActiveStringSet.GetTexts(probe.ref)
        probe.receiveMessage().texts should ===(Set("added to a", "added to b"))
      }
    }

    "store and retrieve snapshot with metadata" in {
      val snapshotStore = Persistence(testKit.system).snapshotStoreFor("akka.persistence.spanner.snapshot")

      import akka.actor.typed.scaladsl.adapter._
      val probe = testKit.createTestProbe[Any]()

      // Somewhat confusing that three things are called meta data, SnapshotMetadata, SnapshotMetadata.metadata and SnapshotWithMetaData.
      // However, at least this is just between the journal impl and Akka persistence, not really user api
      val replicatedMeta = ReplicatedSnapshotMetadata.instanceForSnapshotStoreTest
      val state = "snapshot"

      snapshotStore.tell(
        SaveSnapshot(SnapshotMetadata("meta-1", 100, System.currentTimeMillis(), Some(replicatedMeta)), state),
        probe.ref.toClassic
      )
      probe.expectMessageType[SaveSnapshotSuccess]

      // load most recent snapshot
      snapshotStore.tell(LoadSnapshot("meta-1", SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref.toClassic)
      // get most recent snapshot
      val loaded = probe.expectMessageType[LoadSnapshotResult]
      val selected = loaded match {
        case LoadSnapshotResult(Some(snapshot: SelectedSnapshot), _) => snapshot
        case unexpected => fail(s"Unexpected: $unexpected")
      }
      selected.snapshot should equal(state)
      selected.metadata.metadata should ===(Some(replicatedMeta))
    }
  }
}
