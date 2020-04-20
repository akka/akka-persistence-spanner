package akka.persistence.spanner.internal

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.google.protobuf.struct.Value
import com.google.protobuf.struct.Value.Kind
import com.google.spanner.v1.{PartialResultSet, ResultSetMetadata, StructType}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class RowCollectorSpec
    extends TestKit(ActorSystem(classOf[PartialResultSetDechunkerSpec].getSimpleName))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures {
  "The RowCollector" must {
    "pass through full rows" in {
      val futureResult = Source(
        Seq(
          PartialResultSet(
            metadata = Some(
              ResultSetMetadata(
                rowType = Some(StructType(fields = Seq(StructType.Field("column1"))))
              )
            ),
            values = Seq(
              Value(Kind.StringValue("value1"))
            )
          )
        )
      ).via(RowCollector()).runWith(Sink.seq)

      val rows = futureResult.futureValue
      rows must have size (1)
      rows.head must ===(Seq(Value(Kind.StringValue("value1"))))
    }

    "collect partial rows into full rows" in {
      val futureResult = Source(
        Seq(
          PartialResultSet(
            metadata = Some(
              ResultSetMetadata(
                rowType = Some(StructType(fields = Seq(StructType.Field("column1"), StructType.Field("column2"))))
              )
            ),
            values = Seq(
              Value(Kind.StringValue("value1"))
            )
          ),
          PartialResultSet(
            values = Seq(
              Value(Kind.StringValue("value2"))
            )
          )
        )
      ).via(RowCollector()).runWith(Sink.seq)

      val rows = futureResult.futureValue
      rows must have size (1)
      rows.head must ===(Seq(Value(Kind.StringValue("value1")), Value(Kind.StringValue("value2"))))
    }
  }
}
