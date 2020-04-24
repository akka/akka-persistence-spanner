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

```
CREATE TABLE snapshots (
  persistence_id STRING(MAX) NOT NULL,
  sequence_nr INT64 NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  ser_id INT64 NOT NULL,
  ser_manifest STRING(MAX) NOT NULL,
  snapshot BYTES(MAX)
) PRIMARY KEY (persistence_id, sequence_nr) 
``` 

## Usage

The snapshot plugin is used whenever a snapshot write is triggered through the 
[Akka Persistence APIs](https://doc.akka.io/docs/akka/current/typed/persistence-snapshot.html)