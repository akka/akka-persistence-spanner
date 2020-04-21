/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.internal

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.google.protobuf.struct.{ListValue, Value}
import com.google.protobuf.struct.Value.Kind
import com.google.spanner.v1.{PartialResultSet, ResultSetMetadata, StructType}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}

class RowCollectorSpec
    extends TestKit(ActorSystem(classOf[RowCollectorSpec].getSimpleName))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures {
  case class Example(
      name: String,
      firstPart: PartialResultSet,
      secondPart: PartialResultSet,
      expectedRows: Seq[Seq[Value]]
  )

  "The PartialResultSetFlow" must {
    val examples = Seq(
      Example(
        /*
          Chunked: "Strings are concatenated."
          "foo", "bar" => "foobar"
         */
        name = "strings",
        firstPart = PartialResultSet(
          metadata = Some(
            ResultSetMetadata(
              rowType = Some(StructType(fields = Seq(StructType.Field("column1"))))
            )
          ),
          chunkedValue = true,
          values = Seq(Value(Kind.StringValue("foo")))
        ),
        secondPart = PartialResultSet(values = Seq(Value(Kind.StringValue("bar")))),
        // 1 row, 1 column
        expectedRows = Seq(Seq(Value(Kind.StringValue("foobar"))))
      ),
      Example(
        /*
         Chunked: "Lists of non-strings are concatenated."
         [2, 3], [4] => [2, 3, 4]
         */
        name = "lists of non strings",
        firstPart = PartialResultSet(
          metadata = Some(
            ResultSetMetadata(
              rowType = Some(StructType(fields = Seq(StructType.Field("column1"))))
            )
          ),
          chunkedValue = true,
          values = Seq(
            Value(
              Kind.ListValue(
                ListValue(
                  Seq(Value(Kind.NumberValue(2)), Value(Kind.NumberValue(3)))
                )
              )
            )
          )
        ),
        secondPart = PartialResultSet(values = Seq(Value(Kind.ListValue(ListValue(Seq(Value(Kind.NumberValue(4)))))))),
        expectedRows = Seq(
          // 1 row, 1 column
          Seq(
            Value(
              Kind.ListValue(
                ListValue(
                  Seq(Value(Kind.NumberValue(2)), Value(Kind.NumberValue(3)), Value(Kind.NumberValue(4)))
                )
              )
            )
          )
        )
      ),
      Example(
        /*
          Chunked: "Lists are concatenated, but the last and first elements are merged
          because they are strings."
          ["a", "b"], ["c", "d"] => ["a", "bc", "d"]
         */
        name = "lists of strings",
        firstPart = PartialResultSet(
          metadata = Some(
            ResultSetMetadata(
              rowType = Some(
                StructType(
                  fields = Seq(StructType.Field("column1"))
                )
              )
            )
          ),
          chunkedValue = true,
          values = Seq(
            Value(
              Kind.ListValue(
                ListValue(
                  Seq(Value(Kind.StringValue("a")), Value(Kind.StringValue("b")))
                )
              )
            )
          )
        ),
        secondPart = PartialResultSet(
          values = Seq(
            Value(
              Kind.ListValue(
                ListValue(
                  Seq(Value(Kind.StringValue("c")), Value(Kind.StringValue("d")))
                )
              )
            )
          )
        ),
        expectedRows = Seq(
          // 1 row, 1 column which is a list of 3 values
          Seq(
            Value(
              Kind.ListValue(
                ListValue(
                  Seq(Value(Kind.StringValue("a")), Value(Kind.StringValue("bc")), Value(Kind.StringValue("d")))
                )
              )
            )
          )
        )
      ),
      Example(
        /*
          "Lists are concatenated, but the last and first elements are merged
           because they are lists. Recursively, the last and first elements
           of the inner lists are merged because they are strings."
           ["a", ["b", "c"]], [["d"], "e"] => ["a", ["b", "cd"], "e"]
         */
        name = "nested lists of strings",
        firstPart = PartialResultSet(
          metadata = Some(
            ResultSetMetadata(
              rowType = Some(
                StructType(
                  fields = Seq(StructType.Field("column1"))
                )
              )
            )
          ),
          chunkedValue = true,
          values = Seq(
            Value(
              Kind.ListValue(
                ListValue(
                  Seq(
                    Value(Kind.StringValue("a")),
                    Value(
                      Kind.ListValue(
                        ListValue(
                          Seq(Value(Kind.StringValue("b")), Value(Kind.StringValue("c")))
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        ),
        secondPart = PartialResultSet(
          values = Seq(
            Value(
              Kind.ListValue(
                ListValue(
                  Seq(
                    Value(
                      Kind.ListValue(
                        ListValue(
                          Seq(Value(Kind.StringValue("d")))
                        )
                      )
                    ),
                    Value(Kind.StringValue("e"))
                  )
                )
              )
            )
          )
        ),
        expectedRows = Seq(
          // 1 row, 1 columns which is a list with 3 values
          Seq(
            Value(
              Kind.ListValue(
                ListValue(
                  Seq(
                    Value(Kind.StringValue("a")),
                    Value(
                      Kind.ListValue(
                        ListValue(
                          Seq(Value(Kind.StringValue("b")), Value(Kind.StringValue("cd")))
                        )
                      )
                    ),
                    Value(Kind.StringValue("e"))
                  )
                )
              )
            )
          )
        )
      )
    )

    examples.foreach {
      case Example(name, firstPart, secondPart, expectedCombined) =>
        s"recombine partial resultsets with $name" in {
          val futureResult = Source(List(firstPart, secondPart)).via(RowCollector).runWith(Sink.seq)

          val rows = futureResult.futureValue
          rows must ===(expectedCombined)
        }
    }

    "pass non chunked resultsets as is" in {
      val resultSets = List(
        PartialResultSet(
          metadata = Some(
            ResultSetMetadata(
              rowType = Some(StructType(fields = Seq(StructType.Field("column1"))))
            )
          ),
          values = Seq(Value(Kind.StringValue("ha")))
        ),
        PartialResultSet(
          values = Seq(Value(Kind.StringValue("kk")))
        ),
        PartialResultSet(
          values = Seq(Value(Kind.StringValue("er")))
        )
      )
      val futureResult = Source(resultSets).via(RowCollector).runWith(Sink.seq)
      futureResult.futureValue must ===(resultSets.map(_.values))
    }

    "dechunk more than two chunked subsequent partial resultsets" in {
      val futureResult = Source(
        List(
          PartialResultSet(
            metadata = Some(
              ResultSetMetadata(
                rowType = Some(StructType(fields = Seq(StructType.Field("column1"))))
              )
            ),
            values = Seq(Value(Kind.StringValue("ha"))),
            chunkedValue = true
          ),
          PartialResultSet(
            values = Seq(Value(Kind.StringValue("kk"))),
            chunkedValue = true
          ),
          PartialResultSet(
            values = Seq(Value(Kind.StringValue("er")))
          )
        )
      ).via(RowCollector).runWith(Sink.seq)

      val rows = futureResult.futureValue
      rows must have size (1)
      rows.head must have size (1)
      rows.head.head.kind.stringValue must ===(Some("hakker"))
    }

    "pass through full rows" in {
      val futureResult = Source(
        List(
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
      ).via(RowCollector).runWith(Sink.seq)

      val rows = futureResult.futureValue
      rows must have size (1)
      rows.head must ===(Seq(Value(Kind.StringValue("value1"))))
    }

    "collect partial rows into full rows" in {
      val futureResult = Source(
        List(
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
      ).via(RowCollector).runWith(Sink.seq)

      val rows = futureResult.futureValue
      rows must have size (1)
      rows.head must ===(Seq(Value(Kind.StringValue("value1")), Value(Kind.StringValue("value2"))))
    }

    "collect multiple partial and chunked rows into full rows" in {
      val futureResult = Source(
        List(
          // looks like metadata result set could be empty, so exercise that
          PartialResultSet(
            metadata = Some(
              ResultSetMetadata(
                rowType = Some(
                  StructType(
                    fields = Seq(StructType.Field("column1"), StructType.Field("column2"), StructType.Field("column3"))
                  )
                )
              )
            )
          ),
          PartialResultSet(
            chunkedValue = true,
            values = Seq(
              Value(Kind.StringValue("row1-va"))
            )
          ),
          PartialResultSet(
            values = Seq(
              Value(Kind.StringValue("lue1")),
              Value(Kind.StringValue("row1-value2")),
              Value(Kind.StringValue("row1-value3")),
              Value(Kind.StringValue("row2-value1")) // clean/non-chunked cut
            )
          ),
          PartialResultSet(
            chunkedValue = true,
            values = Seq(
              Value(Kind.StringValue("row2-value2")),
              Value(Kind.StringValue("row2-value3")),
              Value(Kind.StringValue("row3-value1")),
              Value(Kind.StringValue("row3-value2")),
              Value(Kind.StringValue("row3-va"))
            )
          ),
          PartialResultSet(
            values = Seq(
              Value(Kind.StringValue("lue3"))
            )
          )
        )
      ).via(RowCollector).runWith(Sink.seq)

      val rows = futureResult.futureValue
      rows must ===((1 to 3).toSeq.map { row =>
        (1 to 3).map { value =>
          Value(Kind.StringValue(s"row$row-value$value"))
        }
      })
    }
  }
}
