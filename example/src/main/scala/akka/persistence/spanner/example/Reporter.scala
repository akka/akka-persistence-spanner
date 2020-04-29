package akka.persistence.spanner.example

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.pubsub.Topic
import akka.actor.typed.scaladsl.Behaviors

import akka.actor.typed.scaladsl.LoggerOps

object Reporter {
  import ReadSideTopic.ReadSideMetrics
  def apply(topic: ActorRef[Topic.Command[ReadSideMetrics]]): Behavior[ReadSideMetrics] =
    Behaviors.setup { ctx =>
      ctx.log.info("Subscribing to latency stats")
      topic ! Topic.Subscribe(ctx.self)
      Behaviors.receiveMessage[ReadSideTopic.ReadSideMetrics] {
        case ReadSideMetrics(count, max, p99, p50) =>
          ctx.log.infoN("Read side Count: {} Max: {} p99: {} p50: {}", count, max, p99, p50)
          Behaviors.same
      }
    }
}
