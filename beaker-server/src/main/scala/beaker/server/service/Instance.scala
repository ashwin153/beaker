package beaker.server.service

import beaker.client.Client
import beaker.common.util._
import beaker.server.Beaker
import beaker.server.protobuf._

import io.grpc.ServerBuilder

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._

/**
 * A Beaker server.
 *
 * @param beaker Underlying beaker.
 * @param seed Optional seed.
 */
class Instance(beaker: Beaker, seed: Option[Client]) {

  private[this] val server = ServerBuilder
    .forPort(this.beaker.address.port)
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
        val cluster = remote.view()
        cluster.addLearners(this.beaker.address)
        remote.reconfigure(cluster)
      }

      ensure {
        // Bootstrap the instance from a quorum of replicas.
        val cluster = remote.view()
        val clients = cluster.acceptors.map(Client.apply)
        Future.sequence(clients.map(_.scan(this.beaker.repair)))
      }

      ensure {
        // Ensure the instance is added as an acceptor.
        val cluster = remote.view()
        cluster.addAcceptors(this.beaker.address)
        remote.reconfigure(cluster)
      }
    }
  }

  /**
   * Terminates the server.
   */
  def close(): Unit = {
    ensure {
      // Remove the instance from the configuration.
      val cluster   = this.beaker.router.configuration.getCluster
      val learners  = cluster.learners.filterNot(_ == this.beaker.address)
      val acceptors = cluster.acceptors.filterNot(_ == this.beaker.address)
      val updated   = cluster.copy(acceptors = acceptors, learners = learners)

      // Reconfigure the instance out of the configuration.
      this.beaker.router.random(_.reconfigure(updated)).filter(_.successful)
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
  def initial(beaker: Beaker): Instance = new Instance(beaker, None)

  /**
   * Constructs an instance that serves the specified beaker and bootstraps itself using the
   * specified seed. All instances in a cluster except the initial member should be created using
   * in this manner.
   *
   * @param beaker Underlying beaker.
   * @param seed Seed beaker.
   * @return Server instance.
   */
  def apply(beaker: Beaker, seed: Client): Instance = new Instance(beaker, Some(seed))

}