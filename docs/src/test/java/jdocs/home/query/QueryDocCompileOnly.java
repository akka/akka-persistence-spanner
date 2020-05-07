package jdocs.home.query;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.persistence.query.NoOffset;
//#imports
import akka.persistence.query.PersistenceQuery;
import akka.persistence.spanner.javadsl.SpannerReadJournal;

//#imports

public class QueryDocCompileOnly {

    static ActorSystem<?> system = ActorSystem.create(Behaviors.empty(), "Docs");

    static void example() {

        //#create
        SpannerReadJournal queries = PersistenceQuery.get(system).getReadJournalFor(SpannerReadJournal.class, SpannerReadJournal.Identifier());
        //#create

        //#events-by-tag
        queries.currentEventsByTag("tag", NoOffset.getInstance());
        //#events-by-tag

        //#events-by-pid
        queries.currentEventsByPersistenceId("pid1", 1, 101);
        //#events-by-pid
    }
}
