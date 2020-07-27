/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import java.util.Base64

import akka.Done
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.spanner.internal.SpannerJournalInteractions.SerializedWrite
import akka.persistence.spanner.SpannerJournal.WriteFinished
import akka.persistence.spanner.internal.SpannerJournalInteractions.SerializedEventMetadata
import akka.persistence.spanner.internal.{SpannerGrpcClientExtension, SpannerJournalInteractions}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension, Serializers}
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SpannerJournal {
  case class WriteFinished(persistenceId: String, done: Future[_])
}

/**
 * INTERNAL API
 */
@InternalApi
final class SpannerJournal(config: Config, cfgPath: String) extends AsyncWriteJournal {
  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(context.system, classOf[SpannerJournal])

  private val sharedConfigPath = cfgPath.replaceAll("""\.journal$""", "")
  private val serialization: Serialization = SerializationExtension(context.system)
  private val journalSettings = new SpannerSettings(context.system.settings.config.getConfig(sharedConfigPath))

  private val grpcClient = SpannerGrpcClientExtension(system.toTyped).clientFor(sharedConfigPath)

  private val spannerInteractions = new SpannerJournalInteractions(
    grpcClient,
    journalSettings
  )

  // if there are pending writes when an actor restarts we must wait for
  // them to complete before we can read the highest sequence number or we will miss it
  private val writesInProgress = new java.util.HashMap[String, Future[_]]()

  override def receivePluginInternal: Receive = {
    case WriteFinished(pid, f) =>
      writesInProgress.remove(pid, f)
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    def atomicWrite(atomicWrite: AtomicWrite): Future[Try[Unit]] = {
      val serialized: Try[Seq[SerializedWrite]] = Try {
        atomicWrite.payload.map { pr =>
          val (event, tags) = pr.payload match {
            case Tagged(payload, tags) => (payload.asInstanceOf[AnyRef], tags)
            case other => (other.asInstanceOf[AnyRef], Set.empty[String])
          }
          val serialized = serialization.serialize(event).get
          val serializer = serialization.findSerializerFor(event)
          val manifest = Serializers.manifestFor(serializer, event)
          val id: Int = serializer.identifier

          val serializedAsString = Base64.getEncoder.encodeToString(serialized)

          val write = SerializedWrite(
            pr.persistenceId,
            pr.sequenceNr,
            serializedAsString,
            id,
            manifest,
            pr.writerUuid,
            tags,
            None
          )

          if (!journalSettings.metaEnabled) write
          else {
            pr.metadata match {
              case Some(replicatedMeta) =>
                val m = replicatedMeta.asInstanceOf[AnyRef]
                val serializedMeta = serialization.serialize(m).get
                val serializedMetaAsString = Base64.getEncoder.encodeToString(serializedMeta)
                val metaSerializer = serialization.findSerializerFor(m)
                val metaManifest = Serializers.manifestFor(metaSerializer, m)
                val id: Int = metaSerializer.identifier
                write.copy(metadata = Some(SerializedEventMetadata(id, metaManifest, serializedMetaAsString)))
              case None =>
                // meta enabled but regular entity
                write
            }
          }
        }
      }

      log.debug("writing mutations [{}]", serialized)

      serialized match {
        case Success(writes) =>
          spannerInteractions.writeEvents(writes).map(_ => Success(()))(ExecutionContexts.parasitic)
        case Failure(t) => Future.successful(Failure(t))
      }
    }

    val write = Future.sequence(messages.map(aw => atomicWrite(aw)))
    writesInProgress.put(messages.head.persistenceId, write)
    write
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("asyncDeleteMessagesTo [{}] [{}]", persistenceId, toSequenceNr)
    spannerInteractions.deleteMessagesTo(persistenceId, toSequenceNr)
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    log.debug("asyncReplayMessages {} {} {}", persistenceId, fromSequenceNr, toSequenceNr)
    spannerInteractions
      .streamJournal(serialization, persistenceId, fromSequenceNr, toSequenceNr, max)(recoveryCallback)
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("asyncReadHighestSequenceNr [{}] [{}]", persistenceId, fromSequenceNr)
    val pendingWrite = Option(writesInProgress.get(persistenceId)) match {
      case Some(f) =>
        log.debug("Write in progress for {}, deferring highest seq nr until write completed", persistenceId)
        f
      case None => Future.successful(Done)
    }
    pendingWrite.flatMap(_ => spannerInteractions.readHighestSequenceNr(persistenceId, fromSequenceNr))
  }
}
