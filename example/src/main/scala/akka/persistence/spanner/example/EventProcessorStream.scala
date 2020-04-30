package akka.persistence.spanner.example

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.dispatch.ExecutionContexts
import akka.persistence.query.{NoOffset, Offset, PersistenceQuery}
import akka.persistence.spanner.SpannerOffset
import akka.persistence.spanner.internal.SpannerGrpcClientExtension
import akka.persistence.spanner.scaladsl.SpannerReadJournal
import akka.persistence.typed.PersistenceId
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.{RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.google.protobuf.struct.Value.Kind
import com.google.protobuf.struct.{ListValue, Struct, Value}
import com.google.protobuf.struct.Value.Kind.StringValue
import com.google.spanner.v1.{Mutation, Type, TypeCode}
import org.HdrHistogram.Histogram
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

object EventProcessorStream {
  object Schema {
    val offsetStoreTableName = "offsets"

    def offsetStoreTable(): String =
      s"""
       CREATE TABLE $offsetStoreTableName (
        eventProcessorId STRING(MAX) NOT NULL,
        tag STRING(MAX) NOT NULL,
        offset STRING(MAX) NOT NULL,
        seen ARRAY<STRING(MAX)>
      ) PRIMARY KEY (eventProcessorId, tag)
      """

    val EventProcessorId = "eventProcessorId" -> Type(TypeCode.STRING)
    val Tag = "tag" -> Type(TypeCode.STRING)
    val Offset = "offset" -> Type(TypeCode.STRING)
    val Seen = "seen" -> Type(TypeCode.ARRAY)

    val Columns =
      Seq(EventProcessorId, Tag, Offset, Seen)
        .map(_._1)
        .toList

    val offsetQuery =
      s"SELECT offset, seen FROM ${Schema.offsetStoreTableName} WHERE eventProcessorId = @eventProcessorId AND tag = @tag"
    def offsetQueryParams(eventProcessorId: String, tag: String) =
      Struct(
        Map(
          "eventProcessorId" -> Value(StringValue(eventProcessorId)),
          "tag" -> Value(StringValue(tag))
        )
      )
    val offsetQueryParamTypes = Map(
      "eventProcessorId" -> Type(TypeCode.STRING),
      "tag" -> Type(TypeCode.STRING)
    )
  }
}

class EventProcessorStream[Event: ClassTag](
    system: ActorSystem[_],
    executionContext: ExecutionContext,
    eventProcessorId: String,
    tag: String
) {
  protected val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val sys: ActorSystem[_] = system
  implicit val ec: ExecutionContext = executionContext
  import EventProcessorStream.Schema

  private val query = PersistenceQuery(system).readJournalFor[SpannerReadJournal](SpannerReadJournal.Identifier)

  // Note: this client is not public user API
  private val grpcClient = SpannerGrpcClientExtension(sys).clientFor("akka.persistence.spanner")

  def runQueryStream(killSwitch: SharedKillSwitch, histogram: Histogram): Unit =
    RestartSource
      .withBackoff(minBackoff = 500.millis, maxBackoff = 20.seconds, randomFactor = 0.1) { () =>
        Source.futureSource {
          readOffset().map { offset =>
            log.infoN("Starting stream for tag [{}] from offset [{}]", tag, offset)
            processEventsByTag(offset, histogram)
            // groupedWithin can be used here to improve performance by reducing number of offset writes,
            // with the trade-off of possibility of more duplicate events when stream is restarted
              .mapAsync(1)(writeOffset)
          }
        }
      }
      .via(killSwitch.flow)
      .runWith(Sink.ignore)

  private def processEventsByTag(offset: Offset, histogram: Histogram): Source[Offset, NotUsed] =
    query.eventsByTag(tag, offset).mapAsync(1) { eventEnvelope =>
      eventEnvelope.event match {
        case event: Event => {
          // Times from different nodes, take with a pinch of salt
          val latency = System.currentTimeMillis() - eventEnvelope.timestamp
          histogram.recordValue(latency)
          log.debugN(
            "Tag {} Event {} persistenceId {}, sequenceNr {}. Latency {}",
            tag,
            event,
            PersistenceId.ofUniqueId(eventEnvelope.persistenceId),
            eventEnvelope.sequenceNr,
            latency
          )
          Future.successful(Done)
        }.map(_ => eventEnvelope.offset)
        case other =>
          Future.failed(new IllegalArgumentException(s"Unexpected event [${other.getClass.getName}]"))
      }
    }

  private def readOffset(): Future[Offset] =
    grpcClient
      .withSession { implicit session =>
        grpcClient.executeQuery(
          Schema.offsetQuery,
          Schema.offsetQueryParams(eventProcessorId, tag),
          Schema.offsetQueryParamTypes
        )
      }
      .map { rs =>
        if (rs.rows.isEmpty) startOffset()
        else {
          require(rs.rows.size == 1)
          val valueIterator = rs.rows.head.values.iterator
          val offset = valueIterator.next.kind.stringValue.get
          val seen = valueIterator
            .next()
            .kind
            .listValue
            .get
            .values
            .map { value =>
              val string = value.kind.stringValue.get
              val Array(pid, seqnr) = string.split(':')
              pid -> seqnr.toLong
            }
            .toMap

          SpannerOffset(offset, seen)
        }
      }

  private def startOffset(): Offset = NoOffset

  private def writeOffset(offset: Offset)(implicit ec: ExecutionContext): Future[Done] =
    offset match {
      case SpannerOffset(commitTimestamp, seen) =>
        grpcClient
          .withSession { implicit session =>
            grpcClient.write(
              Seq(
                Mutation(
                  Mutation.Operation.InsertOrUpdate(
                    Mutation.Write(
                      Schema.offsetStoreTableName,
                      Schema.Columns,
                      Seq(
                        ListValue(
                          Seq(
                            Value(StringValue(eventProcessorId)),
                            Value(StringValue(tag)),
                            Value(StringValue(commitTimestamp)),
                            Value(Kind.ListValue(ListValue(seen.map {
                              case (pid, seqnr) => Value(StringValue(s"$pid:$seqnr"))
                            }.toSeq)))
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          }
          .map(_ => Done)(ExecutionContexts.parasitic)

      case _ =>
        throw new IllegalArgumentException(s"Unexpected offset type $offset")
    }
}
