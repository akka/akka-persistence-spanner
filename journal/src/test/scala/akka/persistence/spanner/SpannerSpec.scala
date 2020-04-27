/*
 * Copyright (C) 2020 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner

import java.util.concurrent.atomic.AtomicLong

import akka.actor.testkit.typed.internal.CapturingAppender
import akka.actor.testkit.typed.scaladsl.{ActorTestKit}
import akka.actor.typed.scaladsl.adapter._
import akka.grpc.GrpcClientSettings
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
import io.grpc.StatusRuntimeException
import io.grpc.auth.MoreCallCredentials
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, Outcome, Suite}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

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
      akka.loglevel = DEBUG
      akka.actor {
        serialization-bindings {
          "akka.persistence.spanner.CborSerializable" = jackson-cbor 
        }
      }
      akka.persistence.journal.plugin = "akka.persistence.spanner.journal"
      akka.test.timefactor = 2
      # allow java serialization when testing
      akka.actor.allow-java-serialization = on
      akka.actor.warn-about-java-serializer-usage = off
      #instance-config
      akka.persistence.spanner {
        database = ${databaseName.toLowerCase} 
        instance = akka
        project = akka-team
      }
      #instance-config
      
      query {
        refresh-interval = 500ms
      }
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

  val journalTable =
    """
  CREATE TABLE journal (
        persistence_id STRING(MAX) NOT NULL,
        sequence_nr INT64 NOT NULL,
        event BYTES(MAX),
        ser_id INT64 NOT NULL,
        ser_manifest STRING(MAX) NOT NULL,
        tags ARRAY<STRING(MAX)>,
        write_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
        writer_uuid STRING(MAX) NOT NULL,
) PRIMARY KEY (persistence_id, sequence_nr)
  """

  val deleteMetadataTable =
    """
    CREATE TABLE deletions (
        persistence_id STRING(MAX) NOT NULL,
        deleted_to INT64 NOT NULL,
      ) PRIMARY KEY (persistence_id)
  """

  val snapshotTable =
    """
    CREATE TABLE snapshots (
      persistence_id STRING(MAX) NOT NULL,
      sequence_nr INT64 NOT NULL,
      timestamp TIMESTAMP NOT NULL,
      ser_id INT64 NOT NULL,
      ser_manifest STRING(MAX) NOT NULL,
      snapshot BYTES(MAX)
    ) PRIMARY KEY (persistence_id, sequence_nr) 
    """
}

trait SpannerLifecycle
    extends Suite
    with BeforeAndAfterAll
    with AnyWordSpecLike
    with ScalaFutures
    with Matchers
    with Eventually { self =>
  def databaseName: String
  def shouldDumpRows: Boolean = true

  lazy val testKit = ActorTestKit(SpannerSpec.config(databaseName))

  implicit val ec = testKit.system.executionContext
  implicit val defaultPatience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))
  implicit val classicSystem = testKit.system.toClassic

  private val capturingAppender = CapturingAppender.get("")
  private val log = LoggerFactory.getLogger(classOf[SpannerLifecycle])

  private val pidCounter = new AtomicLong(0)
  def nextPid = s"p-${pidCounter.incrementAndGet()}"

  private val tagCounter = new AtomicLong(0)
  def nextTag = s"tag-${tagCounter.incrementAndGet()}"

  val spannerSettings = new SpannerSettings(testKit.system.settings.config.getConfig("akka.persistence.spanner"))
  val grpcSettings: GrpcClientSettings = if (spannerSettings.useAuth) {
    GrpcClientSettings
      .fromConfig("spanner-client")
      .withCallCredentials(
        MoreCallCredentials.from(
          GoogleCredentials
            .getApplicationDefault()
            .createScoped(
              "https://www.googleapis.com/auth/spanner.admin",
              "https://www.googleapis.com/auth/spanner.data"
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

  def withSnapshotStore: Boolean = false

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
        eventually {
          val fail = adminClient
            .getDatabase(GetDatabaseRequest(spannerSettings.fullyQualifiedDatabase))
            .failed
            .futureValue
          databaseNotFound(fail) should ===(true)
        }
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
          SpannerSpec.journalTable ::
          SpannerSpec.deleteMetadataTable ::
          (if (withSnapshotStore)
             SpannerSpec.snapshotTable :: Nil
           else Nil)
        )
      )
    // wait for db to be ready before testing
    eventually {
      adminClient
        .getDatabase(GetDatabaseRequest(spannerSettings.fullyQualifiedDatabase))
        .futureValue
        .state
        .isReady should ===(true)
    }
    log.info("Database ready {}", spannerSettings.database)
  }

  override protected def withFixture(test: NoArgTest): Outcome = {
    log.info(s"Logging started for test [${self.getClass.getName}: ${test.name}]")
    val res = test()
    log.info(s"Logging finished for test [${self.getClass.getName}: ${test.name}] that [$res]")
    if (!(res.isSucceeded || res.isPending)) {
      failed = true
      println(
        s"--> [${Console.BLUE}${self.getClass.getName}: ${test.name}${Console.RESET}] Start of log messages of test that [$res]"
      )
      capturingAppender.flush()
      println(
        s"<-- [${Console.BLUE}${self.getClass.getName}: ${test.name}${Console.RESET}] End of log messages of test that [$res]"
      )
    }
    res
  }

  def cleanup(): Unit = {
    if (failed && shouldDumpRows) {
      log.info("Test failed. Dumping rows")
      val rows = for {
        session <- spannerClient.createSession(CreateSessionRequest(spannerSettings.fullyQualifiedDatabase))
        execute <- spannerClient.executeSql(
          ExecuteSqlRequest(
            session = session.name,
            sql = s"select * from ${spannerSettings.journalTable} order by persistence_id, sequence_nr"
          )
        )
        deletions <- spannerClient.executeSql(
          ExecuteSqlRequest(session = session.name, sql = s"select * from ${spannerSettings.deletionsTable}")
        )
        snapshotRows <- if (withSnapshotStore)
          spannerClient
            .executeSql(
              ExecuteSqlRequest(session = session.name, sql = s"select * from ${spannerSettings.snapshotsTable}")
            )
            .map(_.rows)
        else
          Future.successful(Seq.empty)
        _ <- spannerClient.deleteSession(DeleteSessionRequest(session.name))
      } yield (execute.rows, deletions.rows, snapshotRows)
      val (messageRows, deletionRows, snapshotRows) = rows.futureValue

      def printRow(row: ListValue): Unit =
        log.info("row: {} ", reasonableStringFormatFor(row))

      messageRows.foreach(printRow)
      log.info("Message rows dumped.")
      deletionRows.foreach(printRow)
      log.info("Deletion rows dumped.")
      if (withSnapshotStore) {
        snapshotRows.foreach(printRow)
      }
      log.info("Snapshot rows dumped.")
    }

    adminClient.dropDatabase(DropDatabaseRequest(spannerSettings.fullyQualifiedDatabase))
    log.info("Database dropped {}", spannerSettings.database)
  }

  private def reasonableStringFormatFor(row: ListValue): String =
    row.values
      .map(_.kind match {
        case Kind.ListValue(list) => reasonableStringFormatFor(list)
        case Kind.StringValue(string) =>
          if (string.length <= 50) s"'$string'"
          else s"'${string.substring(0, 50)}...'"
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
      super.afterAll()
      testKit.shutdownTestKit()
    }
}

/**
 * Spec for running a test against spanner.
 *
 * Assumes a locally running spanner, creates and tears down a database.
 */
abstract class SpannerSpec(override val databaseName: String = SpannerSpec.getCallerName(getClass))
    extends SpannerLifecycle {}
