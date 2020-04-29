# End to end example

Port of the Cassandra end to end (https://github.com/akka/akka-persistence-cassandra/pull/748/files#diff-20bfc3ff6a860483887b93bf9cf0d135)

Example tests events by tag end to end.

All events are tagged with a configurable number of tags `tag-1`, `tag-2`, etc.

Then there are N processors that each process a configured number of tags.

The write side will use processor * tags per processors as the toal number of tags

There are three roles:

 * write - run the persistent actors in sharding
 * load - generate load to the persistent actors
 * read - run the sharded daemon set to read the tagged events

The read side periodically publishes latency statistics to distributed pub sub. These are currently just logged out

## Running locally with Spanner emulator

The Spanner emulator cluster must be available on `localhost` port `9010` and `9020)`, use `docker/setup_emulator.sh` to start it.

Note that the emulator only allows one concurrent session (per client?) which both means that it will be slow and that operations are serialized. The latency and performance is not of much relevance for actual usage of the plugin.

The first node can run with the default ports for remoting and akka management. The config location
 and the role must be specified as JVM options:
 
`-Dconfig.resource=local-emulator.conf -Dakka.cluster.roles.0=write`
 
Each node needs its akka management and remoting port overriden. For the cluster to join the default settings with `local-emulator.conf`
 and ports 2552 and 8552 need to be running.
 
`-Dakka.remote.artery.canonical.port=2552 -Dakka.management.http.port=8552 -Dconfig.resource=local-emulator.conf -Dakka.cluster.roles.0=write`

At least one load node:

`-Dakka.remote.artery.canonical.port=2553 -Dakka.management.http.port=8553 -Dconfig.resource=local-emulator.conf -Dakka.cluster.roles.0=load`

And finally at least one read node:

  `-Dakka.remote.artery.canonical.port=2554 -Dakka.management.http.port=8554 -Dconfig.resource=local-emulator.conf -Dakka.cluster.roles.0=read`

 ## Running inside a Kubernetes Cluster
 
 TODO