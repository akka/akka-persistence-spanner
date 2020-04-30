/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import java.io.{File, PrintWriter}

import akka.actor.ActorSystem

object PrintSchema {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("PrintCreateStatements")
    val settings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))

    def withWriter(name: String)(f: PrintWriter => Unit): Unit = {
      val writer: PrintWriter = new PrintWriter(new File(name))
      try {
        f(writer)
      } finally {
        writer.flush()
        writer.close()
      }
    }

    withWriter("./target/journal-tables.txt") { pw =>
      pw.println("//#journal-tables")
      pw.println(SpannerSpec.journalTable(settings))
      pw.println(SpannerSpec.deleteMetadataTable(settings))
      pw.println("//#journal-tables")
    }

    withWriter("./target/snapshot-tables.txt") { pw =>
      pw.println("//#snapshot-tables")
      pw.println(SpannerSpec.snapshotTable(settings))
      pw.println("//#snapshot-tables")
    }
    system.terminate()
  }
}
