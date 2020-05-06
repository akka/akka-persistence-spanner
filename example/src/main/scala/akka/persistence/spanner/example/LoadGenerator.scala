package akka.persistence.spanner.example

import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration
import scala.util.Random
import akka.util.JavaDurationConverters._

object LoadGenerator {
  object Settings {
    def apply(config: Config): Settings =
      Settings(
        config.getInt("persistence-ids"),
        config.getDuration("load-tick-duration").asScala,
        config.getInt("events-per-tick"))
  }

  case class Settings(nrPersistenceIds: Int, tickDuration: FiniteDuration, eventsPerTick: Int)

  sealed trait Command
  final case class Start(duration: FiniteDuration) extends Command
  final case class Tick() extends Command
  private case object Stop extends Command

  def apply(settings: Settings, ref: ActorRef[ShardingEnvelope[ConfigurablePersistentActor.Event]]): Behavior[Command] =
    Behaviors.withTimers { timers =>
      Behaviors.setup { ctx =>
        Behaviors.receiveMessage {
          case Start(duration) =>
            ctx.log.info("Starting...")
            timers.startTimerAtFixedRate(Tick(), settings.tickDuration)
            timers.startSingleTimer(Stop, duration)
            Behaviors.same
          case Tick() =>
            ctx.log.info("Sending [{}] event(s)", settings.eventsPerTick)
            (0 to settings.eventsPerTick).foreach { _ =>
              ref ! ShardingEnvelope(
                s"p${Random.nextInt(settings.nrPersistenceIds)}",
                ConfigurablePersistentActor.Event())
            }
            Behaviors.same
          case Stop =>
            ctx.log.info("Ending load generation")
            Behaviors.stopped
        }
      }
    }
}
