package akka.persistence.spanner.example

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.{Behaviors, LoggerOps}
import akka.cluster.typed.{Cluster, SelfUp, Subscribe}
import akka.grpc.GrpcClientSettings
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.persistence.spanner.SpannerSettings
import com.google.auth.oauth2.GoogleCredentials
import com.google.spanner.admin.database.v1.{CreateDatabaseRequest, DatabaseAdminClient}
import com.google.spanner.admin.instance.v1.{CreateInstanceRequest, InstanceAdminClient}
import com.google.spanner.v1.SpannerClient
import io.grpc.auth.MoreCallCredentials
import akka.persistence.spanner.SpannerSpec

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit =
    ActorSystem(
      Behaviors.setup[Any] { ctx =>
        val cluster = Cluster(ctx.system)
        println("starting up example")
        ctx.log.info("Starting up example")
        if (cluster.selfMember.hasRole("read")) {
          // FIXME create offset store
        }
        if (cluster.selfMember.hasRole("write")) {
          ctx.pipeToSelf(initSpannerInstance(ctx.system))(identity)
        }

        val readSettings = ReadSide.Settings(ctx.system.settings.config.getConfig("spanner.example"))
        val writeSettings = ConfigurablePersistentActor.Settings(readSettings.nrTags)
        val loadSettings = LoadGenerator.Settings(ctx.system.settings.config.getConfig("spanner.example"))

        AkkaManagement(ctx.system).start()
        ClusterBootstrap(ctx.system).start()
        cluster.subscriptions ! Subscribe(ctx.self, classOf[SelfUp])

        val topic = ReadSideTopic.init(ctx)

        Behaviors.receiveMessagePartial {
          case SelfUp(state) =>
            ctx.log.infoN(
              "Cluster member joined. Initializing persistent actors. Roles {}. Members {}",
              cluster.selfMember.roles,
              state.members
            )
            val ref = ConfigurablePersistentActor.init(writeSettings, ctx.system)
            if (cluster.selfMember.hasRole("report")) {
              ctx.spawnAnonymous(Reporter(topic))
            }
            ReadSide(ctx.system, topic, readSettings)
            if (cluster.selfMember.hasRole("load")) {
              ctx.log.info("Starting load generation")
              val load = ctx.spawn(LoadGenerator(loadSettings, ref), "load-generator")
              load ! LoadGenerator.Start(10.seconds)
            }
            Behaviors.empty
        }
      },
      "apc-example"
    )

  def initSpannerInstance(system: ActorSystem[_]): Future[Done] = {
    val spannerSettings = new SpannerSettings(system.settings.config.getConfig("akka.persistence.spanner"))
    import akka.actor.typed.scaladsl.adapter._
    implicit val cs = system.toClassic
    implicit val ec = system.executionContext
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

    val adminClient = DatabaseAdminClient(grpcSettings)
    val instanceClient = InstanceAdminClient(grpcSettings)

    def createInstance() = {
      system.log.info("Creating spanner instance")
      instanceClient
        .createInstance(CreateInstanceRequest(spannerSettings.fullyQualifiedProject, spannerSettings.instance))
        .recover {
          case t if t.getMessage.contains("ALREADY_EXISTS") =>
            "ALREADY_EXISTS"
        }
    }

    def createDatabaseAndTables() = {
      system.log.info("Creating spanner db and tables")
      adminClient
        .createDatabase(
          CreateDatabaseRequest(
            parent = spannerSettings.parent,
            s"CREATE DATABASE ${spannerSettings.database}",
            SpannerSpec.journalTable(spannerSettings) ::
            SpannerSpec.deleteMetadataTable(spannerSettings) ::
            SpannerSpec.snapshotTable(spannerSettings) :: Nil
          )
        )
        .recover {
          case t if t.getMessage.contains("ALREADY_EXISTS") =>
            "ALREADY_EXISTS"
        }
    }

    for {
      _ <- createInstance()
      _ <- createDatabaseAndTables()
    } yield Done
  }
}
