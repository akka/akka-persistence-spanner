# gRPC client and Session Pool

The plugin uses the Spanner gRPC API. When interacting with Spanner over gRPC a session pool needs to be maintained.
Each spanner transaction, read or write, needs to be done in the context of a session.

The default session pool size is 5, which limits the number of concurrent reads and writes to Spanner.
This value will need to be increased if you use-case has a large number of concurrent writes or reads.

Sessions are kept alive indefinitely. Support for dynamic resizing is tracked in [issue 13](https://github.com/akka/akka-persistence-spanner/issues/13).

The maximum value is 100, set by Spanner. [Future versions of the plugin may create multiple gRPC clients to allow
more than 100 sessions at a time](https://github.com/akka/akka-persistence-spanner/issues/44).

## Session configuration 

@@snip [reference.conf](/journal/src/main/resources/reference.conf) { #session-pool }

## gRPC client configuration

@@snip [reference.conf](/journal/src/main/resources/reference.conf) { #grpc  }

See the [gRPC documentation](https://doc.akka.io/docs/akka-grpc/current/client/configuration.html#by-configuration) for configuring the client.

### TLS

You can override the client's gRPC`ssl-config` section
in `akka.grpc.client.spanner-client`.
The [Lightbend SSL Config's documention](https://lightbend.github.io/ssl-config/) for more details.

