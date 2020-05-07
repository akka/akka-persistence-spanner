# Journal plugin

## Schema

The following tables need to be created in the configured database:

@@snip [journal-tables](/target/journal-tables.txt) { #journal-tables } 

## Configuration

To activate the journal plugin, add the following line to your Akka `application.conf`:

```
akka.persistence.journal.plugin = "akka.persistence.spanner.journal"
```

Shared configuration is located under `akka.persistence.spanner.journal`.

Configuration just for the journal is under `akka.persistence.spanner`. You will need to set at least:

```
akka.persistence.spanner {
    project = <spanner project>
    instance = <spanner instance>
    database = <spanner database>
}
```

Authentication details will be picked up from the environment using the standard 
mechanism described [here](https://cloud.google.com/docs/authentication/getting-started).
   
### Reference configuration 

The following can be overridden in your `application.conf`

@@snip [reference.conf](/journal/src/main/resources/reference.conf)

### Testing with the Spanner emulator

The [Spanner Emulator](https://cloud.google.com/spanner/docs/emulator). 
Set the following to connect to it:

```
akka.persistence.journal.plugin = "akka.persistence.spanner.journal"
  akka.persistence.spanner {
    session-pool {
      # emulator only supports a single transaction at a time
      max-size = 1
    }
    
    use-auth = false
 }
akka.grpc.client.spanner-client {
  host = localhost
  port = 9010
  use-tls = false
}
```

You will need to have created the instance and database.

### Deletes

The journal supports deletes through hard deletes, which means the journal entries are actually deleted from the database. 
There is no materialized view with a copy of the event so if an event that is tagged is deleted it will no longer show up in events by tag queries.
