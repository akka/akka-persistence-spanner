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
import akka.persistence.spanner.Schema.Columns
import akka.persistence.spanner.internal.{SessionPool, SpannerGrpcClient}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{SerializationExtension, Serializers}
import com.google.auth.oauth2.GoogleCredentials
import com.google.protobuf.struct.Value.Kind.StringValue
import com.google.protobuf.struct.{ListValue, Struct, Value}
import com.google.spanner.v1.{Mutation, SpannerClient, Type, TypeCode}
import com.typesafe.config.Config
import io.grpc.auth.MoreCallCredentials

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Schema {
  val Table = "messages"
  object Columns {
    val PersistenceId = "persistence_id" -> Type(TypeCode.STRING)
    val SeqNr = "sequence_nr" -> Type(TypeCode.INT64)
    val Payload = "payload" -> Type(TypeCode.BYTES)
    val SerId = "ser_id" -> Type(TypeCode.INT64)
    val SerManifest = "ser_manifest" -> Type(TypeCode.STRING)
    val WriteTime = "write_time" -> Type(TypeCode.TIMESTAMP)
    val WriterUUID = "writer_uuid" -> Type(TypeCode.STRING)
    val Tags = "tags" -> Type(TypeCode.ARRAY)

    val Columns = List(PersistenceId, SeqNr, Payload, SerId, SerManifest, WriteTime, WriterUUID, Tags).map(_._1)
  }

  val ReplaySql =
    s"SELECT ${Columns.Columns.mkString(",")} FROM messages WHERE persistence_id = @persistence_id AND sequence_nr >= @from_sequence_Nr AND sequence_nr <= @to_sequence_nr ORDER BY sequence_nr limit @max"

  val ReplayTypes = Map(
    "persistence_id" -> Type(TypeCode.STRING),
    "from_sequence_nr" -> Type(TypeCode.INT64),
    "to_sequence_nr" -> Type(TypeCode.INT64),
    "max" -> Type(TypeCode.INT64)
  )
}

class SpannerJournal(config: Config) extends AsyncWriteJournal {
  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(context.system, classOf[SpannerJournal])

  private val serialization = SerializationExtension(context.system)
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

  private val spannerGrpcClient = new SpannerGrpcClient(grpcClient, system.toTyped, sessionPool)

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    def atomicWrite(atomicWrite: AtomicWrite): Future[Try[Unit]] = {
      val mutations: Try[Seq[Mutation]] = Try {
        atomicWrite.payload.map { pr =>
          val (payload, tags) = pr.payload match {
            case Tagged(payload, tags) => (payload.asInstanceOf[AnyRef], tags)
            case other => (other.asInstanceOf[AnyRef], Set.empty)
          }
          val serializedTags: List[Value] = tags.map(s => Value(StringValue(s))).toList
          val serialized = serialization.serialize(payload).get
          val serializer = serialization.findSerializerFor(payload)
          val manifest = Serializers.manifestFor(serializer, payload)
          val id: Int = serializer.identifier

          val payloadAsString = Base64.getEncoder.encodeToString(serialized)

          Mutation(
            Mutation.Operation.Insert(
              Mutation.Write(
                journalSettings.table,
                Columns.Columns,
                List(
                  ListValue(
                    List(
                      Value(StringValue(pr.persistenceId)),
                      Value(StringValue(pr.sequenceNr.toString)),
                      Value(StringValue(payloadAsString)),
                      Value(StringValue(id.toString)),
                      Value(StringValue(manifest)),
                      // special value for a timestamp that gets the write timestamp
                      Value(StringValue("spanner.commit_timestamp()")),
                      Value(StringValue(pr.writerUuid)),
                      Value(Value.Kind.ListValue(ListValue(serializedTags)))
                    )
                  )
                )
              )
            )
          )
        }
      }

      log.debug("writing mutations [{}]", mutations)

      mutations match {
        case Success(mutations) => spannerGrpcClient.write(mutations).map(_ => Success(()))
        case Failure(t) => Future.successful(Failure(t))
      }
    }

    Future.sequence(messages.map(aw => atomicWrite(aw)))
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("asyncDeleteMessagesTo [{}] [{}]", persistenceId, toSequenceNr)
    for {
      highestDeletedTo <- findHighestDeletedTo(persistenceId) // user may have passed in a smaller value than previously deleted
      toDeleteTo <- {
        if (toSequenceNr == Long.MaxValue) { // special to delete all but don't set max deleted to it
          asyncReadHighestSequenceNr(persistenceId, highestDeletedTo)
        } else {
          Future.successful(math.max(highestDeletedTo, toSequenceNr))
        }
      }
      _ <- spannerGrpcClient.executeDelete(persistenceId, toDeleteTo)
    } yield ()
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {
    def replayRow(values: Seq[Value], recoveryCallback: PersistentRepr => Unit): Unit = {
      val persistenceId = values.head.getStringValue
      val sequenceNr = values(1).getStringValue.toLong
      val payloadAsString = values(2).getStringValue
      val serId = values(3).getStringValue.toInt
      val serManifest = values(4).getStringValue
      val writeTimestamp = values(5).getStringValue
      val writerUuid = values(6).getStringValue

      val payloadAsBytes = Base64.getDecoder.decode(payloadAsString)
      val payload = serialization.deserialize(payloadAsBytes, serId, serManifest).get
      val repr = PersistentRepr(
        payload,
        sequenceNr,
        persistenceId,
        writerUuid = writerUuid
      )

      log.debug("replaying {}", repr)
      recoveryCallback(repr)
    }

    log.info("asyncReplayMessages {} {} {}", persistenceId, fromSequenceNr, toSequenceNr)
    spannerGrpcClient
      .streamingQuery(
        Schema.ReplaySql,
        params = Struct(
          fields = Map(
            Columns.PersistenceId._1 -> Value(StringValue(persistenceId)),
            "from_sequence_nr" -> Value(StringValue(fromSequenceNr.toString)),
            "to_sequence_nr" -> Value(StringValue(toSequenceNr.toString)),
            "max" -> Value(StringValue(max.toString))
          )
        ),
        paramTypes = Schema.ReplayTypes
      )
      .runForeach(row => replayRow(row, recoveryCallback))
      .map(_ => ())
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("asyncReadHighestSequenceNr [{}] [{}]", persistenceId, fromSequenceNr)
    val maxDeletedTo: Future[Long] = findHighestDeletedTo(persistenceId)
    val maxSequenceNr: Future[Long] = spannerGrpcClient
      .executeQuery(
        "SELECT sequence_nr FROM messages WHERE persistence_id = @persistence_id AND sequence_nr >= @sequence_nr ORDER BY sequence_nr DESC LIMIT 1",
        Struct(
          Map(
            Columns.PersistenceId._1 -> Value(StringValue(persistenceId)),
            Columns.SeqNr._1 -> Value(StringValue(fromSequenceNr.toString))
          )
        ),
        Map(Columns.PersistenceId, Columns.SeqNr)
      )
      .map(
        resultSet =>
          resultSet.rows.size match {
            case 0 =>
              log.info("No rows for persistence id [{}], using fromSequenceNr [{}]", persistenceId, fromSequenceNr)
              fromSequenceNr
            case 1 =>
              val sequenceNr = resultSet.rows.head.values.head.getStringValue.toLong
              log.info("Single row. {}", sequenceNr)
              sequenceNr
            case _ => throw new RuntimeException("More than one row returned from a limit 1 query. " + resultSet)
          }
      )

    for {
      deletedTo <- maxDeletedTo
      max <- maxSequenceNr
    } yield {
      log.debug("Max deleted to [{}] max sequence nr [{}]", deletedTo, max)
      math.max(deletedTo, max)
    }
  }

  private def findHighestDeletedTo(persistenceId: String): Future[Long] =
    spannerGrpcClient
      .executeQuery(
        s"SELECT deleted_to FROM deletions WHERE persistence_id = @persistence_id",
        Struct(
          Map(
            Columns.PersistenceId._1 -> Value(StringValue(persistenceId))
          )
        ),
        Map(Columns.PersistenceId)
      )
      .map { resultSet =>
        if (resultSet.rows.isEmpty) 0L
        else resultSet.rows.head.values.head.getStringValue.toLong
      }
}
