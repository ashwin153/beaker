package beaker.server

import beaker.client.{Client, Cluster}
import beaker.common.util._
import beaker.server.protobuf._

import io.grpc.ServerBuilder

import scala.concurrent.ExecutionContext.Implicits._

/**
 * A Beaker server.
 *
 * @param address Network location.
 * @param beaker Underlying beaker.
 * @param seed Optional seed.
 */
class Instance(
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

    seed foreach { remote =>
      ensure {
        // Ensure the instance is added as a learner.
        remote.network().map(config => remote.reconfigure(config.addLearners(this.address)))
      }

      ensure {
        // Bootstrap the instance from a quorum of replicas.
        remote.network().map(config => Cluster(config.acceptors)).toFuture flatMap { c =>
          c.quorumAsync(_.scan(this.beaker.archive.write)) andThen { case _ => c.close() }
        }
      }

      ensure {
        // Ensure the instance is added as an acceptor.
        remote.network().map(config => remote.reconfigure(config.addAcceptors(this.address)))
      }
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
   * Constructs an instance that serves the specified beaker. Only the initial beaker in a cluster
   * should be created in this manner.
   *
   * @param beaker Underlying beaker.
   * @return Server instance.
   */
  def apply(address: Address, beaker: Beaker): Instance = {
    new Instance(address, beaker, None)
  }

  /**
   * Constructs an instance that serves the specified beaker and bootstraps itself using the
   * specified seed. All instances in a cluster except the initial member should be created using
   * in this manner.
   *
   * @param beaker Underlying beaker.
   * @param seed Seed beaker.
   * @return Server instance.
   */
  def apply(address: Address, beaker: Beaker, seed: Client): Instance = {
    new Instance(address, beaker, Some(seed))
  }

}