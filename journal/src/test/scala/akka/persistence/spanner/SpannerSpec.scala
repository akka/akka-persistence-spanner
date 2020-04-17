/*
 * Copyright (C) 2018-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.actor.ActorSystem
import akka.event.Logging
import akka.grpc.GrpcClientSettings
import akka.testkit.TestKitBase
import com.google.spanner.admin.database.v1.{CreateDatabaseRequest, DatabaseAdminClient, DropDatabaseRequest}
import com.google.spanner.admin.instance.v1.{CreateInstanceRequest, InstanceAdminClient}
import com.google.spanner.v1.{CreateSessionRequest, DeleteSessionRequest, ExecuteSqlRequest, SpannerClient}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, Outcome, Suite}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

object SpannerSpec {
  private var instanceCreated = false
  def ensureInstanceCreated(client: InstanceAdminClient, spannerSettings: SpannerSettings)(
      implicit ec: ExecutionContext
  ): Unit =
    this.synchronized {
      if (!instanceCreated) {
        Await.ready(
          client
            .createInstance(CreateInstanceRequest(spannerSettings.fullyQualifiedProject, spannerSettings.instance))
            .recover {
              case t if t.getMessage.contains("ALREADY_EXISTS") =>
            },
          10.seconds
        )
        instanceCreated = true
      }
    }

  def getCallerName(clazz: Class[_]): String = {
    val s = Thread.currentThread.getStackTrace
      .map(_.getClassName)
      .dropWhile(_.matches("(java.lang.Thread|.*Abstract.*|akka.persistence.spanner.SpannerSpec\\$|.*SpannerSpec)"))
    val reduced = s.lastIndexWhere(_ == clazz.getName) match {
      case -1 => s
      case z => s.drop(z + 1)
    }
    reduced.head.replaceFirst(""".*\.""", "").replaceAll("[^a-zA-Z_0-9]", "_")
  }

  def config(databaseName: String): Config =
    ConfigFactory.parseString(s"""
      akka.persistence.spanner {
        database = $databaseName

        session-pool {
          # emulator only supports a single transaction at a time
          max-size = 1
        }
      }
      akka.grpc.client.spanner-client {
        host = localhost
        port = 9010
        use-tls = false
      }
       """)

  val table =
    """
  CREATE TABLE messages (
        persistence_id STRING(MAX) NOT NULL,
        sequence_nr INT64 NOT NULL,
        payload BYTES(MAX),
        ser_id INT64 NOT NULL,
        ser_manifest STRING(MAX) NOT NULL,
        tags ARRAY<STRING(MAX)>,
        write_time TIMESTAMP OPTIONS (allow_commit_timestamp=true),
        writer_uuid STRING(MAX) NOT NULL,
) PRIMARY KEY (persistence_id, sequence_nr)
     """
}

/**
 * Spec for running a test against spanner.
 *
 * Assumes a locally running spanner, creates and tears down a database.
 */
abstract class SpannerSpec(databaseName: String = SpannerSpec.getCallerName(getClass))
    extends TestKitBase
    with Suite
    with ScalaFutures
    with Matchers
    with BeforeAndAfterAll
    with AnyWordSpecLike {
  final implicit lazy override val system: ActorSystem = ActorSystem(databaseName, SpannerSpec.config(databaseName))
  implicit val ec = system.dispatcher
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))

  val log = Logging(system, classOf[SpannerSpec])
  val spannerSettings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))
  val grpcSettings: GrpcClientSettings = GrpcClientSettings.fromConfig("spanner-client")

  // maybe create these once in an extension when we have lots of tests?
  val adminClient = DatabaseAdminClient(grpcSettings)
  val spannerClient = SpannerClient(grpcSettings)
  val instanceClient = InstanceAdminClient(grpcSettings)

  @volatile private var failed = false

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    SpannerSpec.ensureInstanceCreated(instanceClient, spannerSettings)

    Await.ready(
      adminClient
        .createDatabase(
          CreateDatabaseRequest(
            parent = spannerSettings.parent,
            s"CREATE DATABASE ${spannerSettings.database}",
            List(SpannerSpec.table)
          )
        ),
      10.seconds
    )
    log.info("Database created {}", spannerSettings.database)
  }

  override protected def withFixture(test: NoArgTest): Outcome = {
    val res = test()
    if (!(res.isSucceeded || res.isPending)) {
      failed = true
    }
    res
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    if (failed) {
      log.info("Test failed. Dumping rows")
      val rows = for {
        session <- spannerClient.createSession(CreateSessionRequest(spannerSettings.fullyQualifiedDatabase))
        execute <- spannerClient.executeSql(
          ExecuteSqlRequest(session = session.name, sql = "select * from messages order by persistence_id, sequence_nr")
        )
        _ <- spannerClient.deleteSession(DeleteSessionRequest(session.name))
      } yield execute.rows
      rows.futureValue.foreach(row => log.info("row: {} ", row))
      log.info("Rows dumped.")
    }

    adminClient.dropDatabase(DropDatabaseRequest(spannerSettings.fullyQualifiedDatabase))
    log.info("Database dropped {}", spannerSettings.database)
  }
}
