# Contributing 

Please feel free to contribute to Akka Persistence Spanner and the documentation by reporting issues you identify, or by suggesting changes to the code. 
Please refer to our [contributing instructions](https://github.com/akka/akka/blob/master/CONTRIBUTING.md) to learn how it can be done.

We want Akka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://www.lightbend.com/conduct).

## Running the tests

The tests expect a locally running Spanner Emulator.

It can be started with the docker-comopse file in the docker folder:

@@snip [docker-compose.yml](/docker/docker-compose.yml)

### Running tests against Google Spanner

To run the tests against Spanner set the JVM property `akka.spanner.real-spanner` to "true".

The default configuration uses the project `akka-team` and the instance `akka`. These can be overridden in `SpannerSpec.scala` for all tests.

@@snip [SpannerSpec.scala](/journal/src/test/scala/akka/persistence/spanner/SpannerSpec.scala) { #instance-config } 
