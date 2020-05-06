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

The read side gathers latency statistics, from when the event was written to when it was consumed by the read side (given 
that the node clocks are in sync), the statistics are published to distributed pub sub. These are currently just logged out in millis.

The sample will try to create tables but there is no retrying if it fails for some reason so pay attention to early log entries in case the example does not work.

## Running locally with Spanner emulator

Since the emulator is limited to a single concurrent session it is not possible to run with the emulator. There is config for running with the emulator in `local-emulator.conf` but the example does not work with it.  

## Running locally against cloud spanner

Set up a service account, download private key json and point the `GOOGLE_APPLICATION_CREDENTIALS` env variable to it.
The example will try to create the instance, database and tables.

The first node can run with the default ports for remoting and akka management. The config location
 and the role must be specified as JVM options:
 
```
sbt -Dconfig.resource=local.conf -Dakka.cluster.roles.0=write "example/run"
```
 
Each node needs its akka management and remoting port overriden. For the cluster to join the default settings with `local.conf`
 and ports 2552 and 8552 need to be running.
 
```
sbt -Dakka.remote.artery.canonical.port=2552 -Dakka.management.http.port=8552 -Dconfig.resource=local.conf -Dakka.cluster.roles.0=write "example/run"
```

At least one load node:

```
sbt -Dakka.remote.artery.canonical.port=2553 -Dakka.management.http.port=8553 -Dconfig.resource=local.conf -Dakka.cluster.roles.0=load "example/run"
```

And finally at least one read node, combined with reporting the latency:

```
sbt -Dakka.remote.artery.canonical.port=2554 -Dakka.management.http.port=8554 -Dconfig.resource=local.conf -Dakka.cluster.roles.0=read -Dakka.cluster.roles.1=report "example/run"
```



 ## Running inside a Kubernetes Cluster
 
 TODO