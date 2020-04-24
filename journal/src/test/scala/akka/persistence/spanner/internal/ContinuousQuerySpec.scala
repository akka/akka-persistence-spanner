package akka.persistence.spanner.internal

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration._
import org.scalatest.wordspec.AnyWordSpecLike

class ContinuousQuerySpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with ScalaFutures {
  implicit val as: ActorSystem = system.classicSystem
  "ContinuousQuery" should {
    "work for initial query" in {
      ContinuousQuery[String, String](
        "cats",
        (_, _) => "cats",
        results(Source(List("one", "two", "three"))),
        0,
        1.second
      ).runWith(Sink.seq).futureValue shouldEqual List("one", "two", "three")
    }
    "complete if none returned" in {
      ContinuousQuery[String, String]("cats", (_, _) => "cats", _ => None, 0, 1.second)
        .runWith(Sink.seq)
        .futureValue shouldEqual Nil
    }
    "execute next query on complete" in {
      ContinuousQuery[String, String](
        "cats",
        (_, _) => "cats",
        results(Source(List("one", "two")), Source(List("three", "four"))),
        1,
        1.second
      ).runWith(Sink.seq).futureValue shouldEqual List("one", "two", "three", "four")
    }

    "buffer element if no demand" in {
      val sub = ContinuousQuery[String, String](
        "cats",
        (_, _) => "cats",
        results(Source(List("one", "two")), Source(List("three", "four"))),
        0,
        1.second
      ).runWith(TestSink.probe[String])

      sub
        .request(1)
        .expectNext("one")
        .expectNoMessage()
        .request(1)
        .expectNext("two")
        .request(3)
        .expectNext("three")
        .expectNext("four")
        .expectComplete()
    }

    "update state every element" in {
      pending
    }

    def results(results: Source[String, NotUsed]*): String => Option[Source[String, NotUsed]] = {
      var r = results.toList
      def next(state: String): Option[Source[String, NotUsed]] =
        r match {
          case x :: xs =>
            r = xs
            Some(x)
          case Nil => None
        }
      next
    }
  }
}
