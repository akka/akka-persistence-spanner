# Query Plugin

It implements the following @extref:[Persistence Queries](akka:persistence-query.html):

* eventsByPersistenceId, currentEventsByPersistenceId
* eventsByTag, currentEventsByTag
* persistenceIds, currentPersistenceIds 

Accessing the PersistenceQuery for spanner:

Java
:  @@snip [create](/docs/src/test/java/jdocs/home/query/QueryDocCompileOnly.java) { #imports #create } 

Scala
:  @@snip [create](/docs/src/test/scala/docs/home/query/QueryDocCompileOnly.scala) { #create } 
    
## Configuration

Query configuration is under `akka.persistence.spanner.query`. Here's the default configuration 
values for the query plugin:

@@snip [reference.conf](/journal/src/main/resources/reference.conf) { #query }

The query plugin shares the same gRPC client and session pool as the rest of the plugin. See @ref:[tuning the session pool](tuning-session-pool.md)
for configuration.

