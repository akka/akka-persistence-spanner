/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner

import java.io.{File, PrintWriter}
import akka.actor.ActorSystem
import akka.persistence.spanner.internal.{
  SpannerJournalInteractions,
  SpannerObjectInteractions,
  SpannerSnapshotInteractions
}

object PrintSchema {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("PrintCreateStatements")
    val settings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))

    def withWriter(name: String)(f: PrintWriter => Unit): Unit = {
      val writer: PrintWriter = new PrintWriter(new File(name))
      try f(writer)
      finally {
        writer.flush()
        writer.close()
      }
    }

    withWriter("./target/journal-tables.txt") { pw =>
      pw.println("//#journal-tables")
      pw.println(SpannerJournalInteractions.Schema.Journal.journalTable(settings))
      pw.println("")
      pw.println(SpannerJournalInteractions.Schema.Tags.tagTable(settings))
      pw.println("")
      pw.println(SpannerJournalInteractions.Schema.Tags.eventsByTagIndex(settings))
      pw.println("")
      pw.println(SpannerJournalInteractions.Schema.Deleted.deleteMetadataTable(settings))
      pw.println("//#journal-tables")
    }

    withWriter("./target/snapshot-tables.txt") { pw =>
      pw.println("//#snapshot-tables")
      pw.println(SpannerSnapshotInteractions.Schema.Snapshots.snapshotTable(settings))
      pw.println("//#snapshot-tables")
    }

    withWriter("./target/object-tables.txt") { pw =>
      pw.println("//#object-tables")
      pw.println(SpannerObjectInteractions.Schema.Objects.objectTable(settings))
      pw.println("//#object-tables")
    }
    system.terminate()
  }
}
