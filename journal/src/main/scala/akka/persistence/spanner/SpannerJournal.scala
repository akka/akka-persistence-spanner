/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import java.util.Base64

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.event.Logging
import akka.grpc.GrpcClientSettings
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.spanner.SpannerInteractions.SerializedWrite
import akka.persistence.spanner.internal.{SessionPool, SpannerGrpcClient}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension, Serializers}
import com.google.auth.oauth2.GoogleCredentials
import com.google.spanner.v1.SpannerClient
import com.typesafe.config.Config
import io.grpc.auth.MoreCallCredentials

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class SpannerJournal(config: Config) extends AsyncWriteJournal {
  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(context.system, classOf[SpannerJournal])

  private val serialization: Serialization = SerializationExtension(context.system)
  private val journalSettings = new SpannerSettings(config)

  private val grpcClient: SpannerClient =
    if (journalSettings.useAuth) {
      SpannerClient(
        GrpcClientSettings
          .fromConfig(journalSettings.grpcClient)
          .withCallCredentials(
            MoreCallCredentials.from(
              GoogleCredentials.getApplicationDefault
                .createScoped("https://www.googleapis.com/auth/spanner")
            )
          )
      )
    } else {
      SpannerClient(GrpcClientSettings.fromConfig("spanner-client"))
    }

  // TODO supervision
  // TODO shutdown?
  private val sessionPool = context.spawn(SessionPool.apply(grpcClient, journalSettings), "session-pool")

  private val spannerInteractions = new SpannerInteractions(
    new SpannerGrpcClient(grpcClient, system.toTyped, sessionPool, journalSettings),
    journalSettings
  )

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
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

          SerializedWrite(
            pr.persistenceId,
            pr.sequenceNr,
            serializedAsString,
            id,
            manifest,
            pr.writerUuid,
            tags
          )
        }
      }

      log.debug("writing mutations [{}]", serialized)

      serialized match {
        case Success(writes) =>
          spannerInteractions.writeEvents(writes).map(_ => Success(()))(ExecutionContext.parasitic)
        case Failure(t) => Future.successful(Failure(t))
      }
    }

    Future.sequence(messages.map(aw => atomicWrite(aw)))
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
    spannerInteractions.readHighestSequenceNr(persistenceId, fromSequenceNr)
  }
}
