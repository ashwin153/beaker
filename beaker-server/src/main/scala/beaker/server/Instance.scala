package beaker.server

import beaker.client.{Client, Cluster}
import beaker.common.util._
import beaker.server.protobuf._

import io.grpc.ServerBuilder
import pureconfig._

import java.net.InetAddress
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._

/**
 * A Beaker server.
 *
 * @param address Network location.
 * @param beaker Underlying beaker.
 * @param seed Optional seed.
 */
case class Instance(
  address: Address,
  beaker: Beaker,
  seed: Option[Client]
) {

  private[this] val server = ServerBuilder
    .forPort(this.address.port)
    .addService(BeakerGrpc.bindService(this.beaker, global))
    .build()

  /**
   * Asynchronously serves the beaker.
   */
  def serve(): Unit = {
    this.server.start()

    seed match {
      case None =>
        // Reconfigure the beaker with the default initial configuration.
        val ballot  = this.beaker.proposer.after(this.beaker.proposer.view.ballot)
        val initial = View(ballot, Configuration(0.51, Seq(this.address), Seq(this.address)))
        this.beaker.proposer.reconfigure(initial)
      case Some(remote) =>
        ensure {
          // Ensure the instance is added as a learner.
          remote.network() map { view =>
            remote.reconfigure(view.configuration.addLearners(this.address))
          }
        }

        ensure {
          // Bootstrap the instance from a quorum of replicas.
          remote.network().toFuture map { view =>
            this.beaker.proposer.reconfigure(view)
            this.beaker.proposer.acceptors.quorumAsync(_.scan(this.beaker.archive.write))
          }
        }

        ensure {
          // Ensure the instance is added as an acceptor.
          remote.network() map { view =>
            remote.reconfigure(view.configuration.addAcceptors(this.address))
          }
        }

        // Terminate the connection to the seed.
        remote.close()
    }
  }

  /**
   * Terminates the server.
   */
  def close(): Unit = {
    ensure {
      // Remove the instance from the configuration.
      val configuration = this.beaker.proposer.configuration
      val acceptors = configuration.acceptors.remove(this.address)
      val learners = configuration.learners.remove(this.address)
      val updated  = configuration.copy(acceptors = acceptors, learners = learners)

      // Reconfigure the instance out of the configuration.
      val cluster = Cluster(configuration.acceptors)
      cluster.random(_.reconfigure(updated)) andThen { case _ => cluster.close() }
    } map { _ =>
      // Shutdown the instance.
      this.server.shutdown()
    }
  }

}

object Instance {

  /**
   * An instance configuration.
   *
   * @param port Port number.
   * @param seed Optional seed location.
   * @param backoff Backoff duration.
   * @param caches Cache hierarchy.
   * @param database Underlying database.
   */
  case class Config(
    port: Int,
    seed: Option[Address],
    backoff: Duration,
    caches: List[String],
    database: String
  )

  /**
   * Constructs an instance from the specified configuration.
   *
   * @param config Configuration.
   * @return Unstarted Instance.
   */
  def apply(config: Instance.Config): Instance = {
    val address = Address(InetAddress.getLocalHost.getHostName, config.port)
    val storage = config.caches.foldRight(Database.forName(config.database))(Cache.forName)
    val beaker  = Beaker(Archive(storage), Proposer(address, config.backoff))
    Instance(address, beaker, config.seed.map(Client.apply))
  }

  /**
   * Asynchronously serves an instance, and automatically closes it on system shutdown. Instances
   * are bootstrapped from the configuration available on the classpath, but this configuration can
   * be explicitly overridden by modifying system properties from the command line.
   *
   * @param args None.
   */
  def main(args: Array[String]): Unit = {
    val instance = Instance(loadConfigOrThrow[Instance.Config])
    instance.serve()
    sys.addShutdownHook(instance.close())
  }

}