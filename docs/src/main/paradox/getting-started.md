# Getting Started

## Dependencies

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-spanner_$scala.binary.version$
  version=$project.version$
}

Configuring as a @ref:[Journal](journal.md)

This plugin depends on Akka 2.6.x and note that it is important that all `akka-*` 
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems 
with transient dependencies causing an unlucky mix of versions.

The plugin is published for Scala 2.13 and 2.12.

