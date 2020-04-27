# Snapshot plugin

The snapshot plugin enables storing and loading snapshots for persistent actors in Spanner.

## Configuration

To activate the journal plugin, add the following line to your Akka `application.conf`:

```
akka.persistence.snapshot-store.plugin = "akka.persistence.spanner.snapshot"
```

The connection settings are shared with the journal plugin, see Journal Plugin for details.

See [reference.conf](https://github.com/akka/akka-persistence-spanner/blob/master/journal/src/main/resources/reference.conf) for complete configuration option docs and defaults

## Schema

The plugin will need a table defined like this:

@@snip [snapshot-tables](/target/snapshot-tables.txt) { #snapshot-tables } 

## Usage

The snapshot plugin is used whenever a snapshot write is triggered through the 
[Akka Persistence APIs](https://doc.akka.io/docs/akka/current/typed/persistence-snapshot.html)