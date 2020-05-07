# gRPC client and Session Pool

The plugin uses the Spanner gRPC API. When interacting with Spanner over gRPC a session pool needs to be maintained.
Each spanner transaction, read or write, needs to be done in the context of a session.

The default session pool size is 5, which limits the number of concurrent reads and writes to Spanner.
This value will need to be increased if you use-case has a large number of concurrent writes or reads.

Sessions are kept alive indefinitely. A dynamic session pool is planned for future versions of the plugin so.

The maximum value is 100, set by Spanner. Future versions of the plugin may create multiple gRPC clients to allow
more than 100 sessions at a time.

## Session configuration 

@@snip [reference.conf](/journal/src/main/resources/reference.conf) { #session-pool }

## gRPC client configuration

@@snip [reference.conf](/journal/src/main/resources/reference.conf) { #grpc  }
