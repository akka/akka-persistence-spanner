# Journal plugin


## Configuration

To activate the journal plugin, add the following line to your Akka `application.conf`:

```
akka.persistence.journal.plugin = "akka.persistence.spanner"
```

In addition the following will need to be set:

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
akka.persistence.journal.plugin = "akka.persistence.spanner"
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



