package akka.persistence.spanner.internal

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.google.protobuf.struct.{ListValue, Value}
import com.google.protobuf.struct.Value.Kind
import com.google.spanner.v1.PartialResultSet
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}

class PartialResultSetFlowSpec
    extends TestKit(ActorSystem(classOf[PartialResultSetFlowSpec].getSimpleName))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures {
  case class Example(name: String, firstPart: Kind, secondPart: Kind, expectedCombined: Kind)

  "The PartialResultSetFlow" must {
    val examples = Seq(
      Example(
        /*
          "Strings are concatenated."
          "foo", "bar" => "foobar"
         */
        name = "strings",
        firstPart = Kind.StringValue("foo"),
        secondPart = Kind.StringValue("bar"),
        expectedCombined = Kind.StringValue("foobar")
      ),
      Example(
        /*
         "Lists of non-strings are concatenated."
         [2, 3], [4] => [2, 3, 4]
         */
        name = "lists of non strings",
        firstPart = Kind.ListValue(
          ListValue(
            Seq(Value(Kind.NumberValue(2)), Value(Kind.NumberValue(3)))
          )
        ),
        secondPart = Kind.ListValue(ListValue(Seq(Value(Kind.NumberValue(4))))),
        expectedCombined = Kind.ListValue(
          ListValue(
            Seq(Value(Kind.NumberValue(2)), Value(Kind.NumberValue(3)), Value(Kind.NumberValue(4)))
          )
        )
      ),
      Example(
        /*
          "Lists are concatenated, but the last and first elements are merged
          because they are strings."
          ["a", "b"], ["c", "d"] => ["a", "bc", "d"]
         */
        name = "lists of strings",
        firstPart = Kind.ListValue(
          ListValue(
            Seq(Value(Kind.StringValue("a")), Value(Kind.StringValue("b")))
          )
        ),
        secondPart = Kind.ListValue(
          ListValue(
            Seq(Value(Kind.StringValue("c")), Value(Kind.StringValue("d")))
          )
        ),
        expectedCombined = Kind.ListValue(
          ListValue(
            Seq(Value(Kind.StringValue("a")), Value(Kind.StringValue("bc")), Value(Kind.StringValue("d")))
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
        firstPart = Kind.ListValue(
          ListValue(
            Seq(
              Value(Kind.ListValue(ListValue(Seq(Value(Kind.StringValue("a")))))),
              Value(
                Kind.ListValue(
                  ListValue(
                    Seq(
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
          )
        ),
        secondPart = Kind.ListValue(
          ListValue(
            Seq(
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
                      )
                    )
                  )
                )
              ),
              Value(Kind.ListValue(ListValue(Seq(Value(Kind.StringValue("e"))))))
            )
          )
        ),
        expectedCombined = Kind.ListValue(
          ListValue(
            Seq(
              Value(Kind.ListValue(ListValue(Seq(Value(Kind.StringValue("a")))))),
              Value(
                Kind.ListValue(
                  ListValue(
                    Seq(
                      Value(
                        Kind.ListValue(
                          ListValue(
                            Seq(Value(Kind.StringValue("b")), Value(Kind.StringValue("cd")))
                          )
                        )
                      )
                    )
                  )
                )
              ),
              Value(Kind.ListValue(ListValue(Seq(Value(Kind.StringValue("e"))))))
            )
          )
        )
      )
    )

    examples.foreach {
      case Example(name, firstPart, secondPart, expectedCombined) =>
        s"recombine values $name" in {
          PartialResultSetFlow.recombine(Value(firstPart), Value(secondPart)) must ===(Value(expectedCombined))
        }

        s"recombine partial resultsets with $name" in {
          val futureResult = Source(
            Seq(
              PartialResultSet(
                values = Seq(Value(firstPart)),
                chunkedValue = true
              ),
              PartialResultSet(
                values = Seq(Value(secondPart))
              )
            )
          ).via(PartialResultSetFlow).runWith(Sink.seq)

          val resultSets = futureResult.futureValue
          resultSets must have size (1)
          resultSets.head.values must have size (1)
          resultSets.head.values.head.kind must ===(expectedCombined)
        }
    }

    "pass non chunked resulsets as is" in {
      val resultSets = Seq(
        PartialResultSet(
          values = Seq(Value(Kind.StringValue("ha")))
        ),
        PartialResultSet(
          values = Seq(Value(Kind.StringValue("kk")))
        ),
        PartialResultSet(
          values = Seq(Value(Kind.StringValue("er")))
        )
      )
      val futureResult = Source(resultSets).via(PartialResultSetFlow).runWith(Sink.seq)
      futureResult.futureValue must ===(resultSets)
    }

    "recombine more than two chunked subsequent partial resultsets" in {
      val futureResult = Source(
        Seq(
          PartialResultSet(
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
      ).via(PartialResultSetFlow).runWith(Sink.seq)

      val resultSets = futureResult.futureValue
      resultSets must have size (1)
      resultSets.head.values must have size (1)
      resultSets.head.values.head.kind.stringValue must ===(Some("hakker"))
    }
  }
}
