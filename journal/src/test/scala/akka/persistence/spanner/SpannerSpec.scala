/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.actor.ActorSystem
import akka.event.Logging
import akka.grpc.GrpcClientSettings
import akka.testkit.{TestKit, TestKitBase}
import com.google.auth.oauth2.GoogleCredentials
import com.google.protobuf.struct.ListValue
import com.google.protobuf.struct.Value.Kind
import com.google.spanner.admin.database.v1.{
  CreateDatabaseRequest,
  DatabaseAdminClient,
  DropDatabaseRequest,
  GetDatabaseRequest
}
import com.google.spanner.admin.instance.v1.{CreateInstanceRequest, InstanceAdminClient}
import com.google.spanner.v1.{CreateSessionRequest, DeleteSessionRequest, ExecuteSqlRequest, SpannerClient}
import com.typesafe.config.{Config, ConfigFactory}
import io.grpc.Status.Code
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.auth.MoreCallCredentials
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.exceptions.TestFailedException
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

  def realSpanner: Boolean = System.getProperty("akka.spanner.real-spanner", "false").toBoolean

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

  def config(databaseName: String): Config = {
    val c = ConfigFactory.parseString(s"""
      akka.persistence.journal.plugin = "akka.persistence.spanner.journal"
      akka.test.timefactor = 2
      #instance-config
      akka.persistence.spanner {
        database = ${databaseName.toLowerCase} 
        instance = akka
        project = akka-team
      }
      #instance-config
       """)
    if (realSpanner) {
      println("running with real spanner")
      c
    } else {
      println("running against emulator")
      c.withFallback(emulatorConfig)
    }
  }

  val emulatorConfig = ConfigFactory.parseString("""
      akka.persistence.spanner {
        session-pool {
          max-size = 1
        }
        use-auth = false
      }
      akka.grpc.client.spanner-client {
        host = localhost
        port = 9010
        use-tls = false
      }
     """)

  def journalTable(settings: SpannerSettings): String =
    s"""CREATE TABLE ${settings.table} (
        persistence_id STRING(MAX) NOT NULL,
        sequence_nr INT64 NOT NULL,
        event BYTES(MAX),
        ser_id INT64 NOT NULL,
        ser_manifest STRING(MAX) NOT NULL,
        tags ARRAY<STRING(MAX)>,
        write_time TIMESTAMP OPTIONS (allow_commit_timestamp=true),
        writer_uuid STRING(MAX) NOT NULL,
) PRIMARY KEY (persistence_id, sequence_nr)
"""

  def deleteMetadataTable(settings: SpannerSettings): String =
    s"""CREATE TABLE ${settings.deletionsTable} (
    persistence_id STRING(MAX) NOT NULL,
    deleted_to INT64 NOT NULL,
) PRIMARY KEY (persistence_id)
"""
}

trait SpannerLifecycle
    extends TestKitBase
    with Suite
    with BeforeAndAfterAll
    with AnyWordSpecLike
    with ScalaFutures
    with Matchers {
  def databaseName: String

  implicit val ec = system.dispatcher
  implicit val defaultPatience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))

  val log = Logging(system, classOf[SpannerSpec])
  val spannerSettings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))
  val grpcSettings: GrpcClientSettings = if (spannerSettings.useAuth) {
    GrpcClientSettings
      .fromConfig("spanner-client")
      .withCallCredentials(
        MoreCallCredentials.from(
          GoogleCredentials
            .getApplicationDefault()
            .createScoped(
              "https://www.googleapis.com/auth/spanner.admin"
            )
        )
      )
  } else {
    GrpcClientSettings.fromConfig("spanner-client")
  }
  // maybe create these once in an extension when we have lots of tests?
  val adminClient = DatabaseAdminClient(grpcSettings)
  val spannerClient = SpannerClient(grpcSettings)
  val instanceClient = InstanceAdminClient(grpcSettings)

  @volatile private var failed = false

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    def databaseNotFound(t: Throwable): Boolean =
      t match {
        case s: StatusRuntimeException =>
          s.getStatus.getCode == Code.NOT_FOUND
        case _ => false
      }
    // create db
    SpannerSpec.ensureInstanceCreated(instanceClient, spannerSettings)
    try {
      val db = adminClient.getDatabase(GetDatabaseRequest(spannerSettings.fullyQualifiedDatabase)).futureValue
      if (db.state.isReady) {
        // there is a db already, drop it to make sure we don't leak db contents from previous run
        log.info("Dropping pre-existing database {}", spannerSettings.fullyQualifiedDatabase)
        adminClient.dropDatabase(DropDatabaseRequest(spannerSettings.fullyQualifiedDatabase))
        awaitAssert(
          {
            val fail = adminClient
              .getDatabase(GetDatabaseRequest(spannerSettings.fullyQualifiedDatabase))
              .failed
              .futureValue
            databaseNotFound(fail) should ===(true)
          },
          20.seconds
        )
      }
    } catch {
      case te: TestFailedException if te.cause.exists(databaseNotFound) =>
      // ok, no pre-existing database
    }

    log.info("Creating database {}", spannerSettings.database)
    adminClient
      .createDatabase(
        CreateDatabaseRequest(
          parent = spannerSettings.parent,
          s"CREATE DATABASE ${spannerSettings.database}",
          List(SpannerSpec.journalTable(spannerSettings), SpannerSpec.deleteMetadataTable(spannerSettings))
        )
      )
    // wait for db to be ready before testing
    awaitAssert({
      adminClient
        .getDatabase(GetDatabaseRequest(spannerSettings.fullyQualifiedDatabase))
        .futureValue
        .state
        .isReady should ===(true)
    }, 20.seconds)
    log.info("Database ready {}", spannerSettings.database)
  }

  override protected def withFixture(test: NoArgTest): Outcome = {
    val res = test()
    if (!(res.isSucceeded || res.isPending)) {
      failed = true
    }
    res
  }

  def cleanup(): Unit = {
    if (failed) {
      log.info("Test failed. Dumping rows")
      val rows = for {
        session <- spannerClient.createSession(CreateSessionRequest(spannerSettings.fullyQualifiedDatabase))
        execute <- spannerClient.executeSql(
          ExecuteSqlRequest(
            session = session.name,
            sql = s"select * from ${spannerSettings.table} order by persistence_id, sequence_nr"
          )
        )
        deletions <- spannerClient.executeSql(
          ExecuteSqlRequest(session = session.name, sql = s"select * from ${spannerSettings.deletionsTable}")
        )
        _ <- spannerClient.deleteSession(DeleteSessionRequest(session.name))
      } yield (execute.rows, deletions.rows)
      val (messageRows, deletionRows) = rows.futureValue
      messageRows.foreach(row => log.info("row: {} ", reasonableStringFormatFor(row)))
      log.info("Message rows dumped.")
      deletionRows.foreach(row => log.info("row: {} ", reasonableStringFormatFor(row)))
      log.info("Deletion rows dumped.")
    }

    adminClient.dropDatabase(DropDatabaseRequest(spannerSettings.fullyQualifiedDatabase))
    log.info("Database dropped {}", spannerSettings.database)
  }

  private def reasonableStringFormatFor(row: ListValue): String =
    row.values
      .map(_.kind match {
        case Kind.ListValue(list) => reasonableStringFormatFor(list)
        case Kind.StringValue(string) => s"'$string'"
        case Kind.BoolValue(bool) => bool.toString
        case Kind.NumberValue(nr) => nr.toString
        case Kind.NullValue(_) => "null"
        case Kind.Empty => ""
        case Kind.StructValue(_) => ???
      })
      .mkString("[", ", ", "]")

  override protected def afterAll(): Unit =
    try {
      cleanup()
    } finally {
      TestKit.shutdownActorSystem(system)
      super.afterAll()
    }
}

/**
 * Spec for running a test against spanner.
 *
 * Assumes a locally running spanner, creates and tears down a database.
 */
abstract class SpannerSpec(override val databaseName: String = SpannerSpec.getCallerName(getClass))
    extends SpannerLifecycle {
  final implicit lazy override val system: ActorSystem = ActorSystem(databaseName, SpannerSpec.config(databaseName))
}
