/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.Done
import akka.actor.typed.{ActorRef, Behavior}
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
        (_, command) => {
          Effect.persist(command).thenRun(_ => command.replyTo ! Done)
        },
        (_, _) => ""
      ).withTagger {
        case WithTags(message, tags, _) => tags
      }
  }

  object Persister {
    case class PersistMe(payload: Any, replyTo: ActorRef[Done])

    def apply(pid: String): Behavior[PersistMe] =
      EventSourcedBehavior[PersistMe, Any, String](
        persistenceId = PersistenceId.ofUniqueId(pid),
        "",
        (_, command) => {
          Effect.persist(command.payload).thenRun(_ => command.replyTo ! Done)
        },
        (_, _) => ""
      )
  }
}
