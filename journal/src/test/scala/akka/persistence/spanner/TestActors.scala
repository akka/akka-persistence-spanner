/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.Done
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.spanner.TestActors.Tagger.Command
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}

object TestActors {
  object Tagger {
    sealed trait Command extends CborSerializable {
      val replyTo: ActorRef[Done]
      val message: String
    }
    case class WithTags(message: String, set: Set[String], replyTo: ActorRef[Done]) extends Command

    def apply(pid: String): Behavior[Tagger.Command] =
      EventSourcedBehavior[Command, Command, String](
        persistenceId = PersistenceId.ofUniqueId(pid),
        "",
        (_, command) => Effect.persist(command).thenRun(_ => command.replyTo ! Done),
        (_, _) => ""
      ).withTagger { case WithTags(message, tags, _) =>
        tags
      }
  }

  object Persister {
    sealed trait Command
    case class PersistMe(payload: Any, replyTo: ActorRef[Done]) extends Command
    case class Ping(replyTo: ActorRef[Done]) extends Command

    def apply(pid: String): Behavior[Command] =
      EventSourcedBehavior[Command, Any, String](
        persistenceId = PersistenceId.ofUniqueId(pid),
        "",
        (_, command) =>
          command match {
            case command: PersistMe =>
              Effect.persist(command.payload).thenRun(_ => command.replyTo ! Done)
            case Ping(replyTo) =>
              replyTo ! Done
              Effect.none
          },
        (_, _) => ""
      )
  }
}
