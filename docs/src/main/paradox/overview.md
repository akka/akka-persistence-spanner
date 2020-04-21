# Overview

The Akka Persistence Spanner  plugin allows for using Google Spanner as a backend for Akka Persistence. 

The current version does not support PersistentQuery but future ones will.

It interacts with Google Spanner asynchronously via [Akka gRPC](https://doc.akka.io/docs/akka-grpc/current/index.html). 

@@@ warning

The project is currently under development and there are no guarantees for binary compatibility
and the schema may change.

@@@

## Project Info

@@project-info{ projectId="journal" }

## Dependencies

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-spanner_$scala.binary.version$
  version=$project.version$
}

This plugin depends on Akka 2.6.x and note that it is important that all `akka-*` 
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems 
with transient dependencies causing an unlucky mix of versions.

@@dependencies{ projectId="journal" }


