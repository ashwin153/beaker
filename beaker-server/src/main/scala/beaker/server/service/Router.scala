package beaker.server.service

import beaker.common.concurrent.Locking
import beaker.server.protobuf.BeakerGrpc
import beaker.server.protobuf._
import beaker.server.service.Router._

import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.collection.mutable
import scala.util.{Random, Try}

/**
 * A thread-safe, request router.
 *
 * @param internal Network configuration.
 * @param acceptors Connections to acceptors.
 * @param learners Connections to learners.
 */
class Router(
  private[this] var internal: Configuration,
  acceptors: mutable.Map[Address, ManagedChannel],
  learners: mutable.Map[Address, ManagedChannel]
) extends Locking {

  reconfigure(this.internal)

  /**
   * Returns the underlying configuration.
   *
   * @return Network configuration.
   */
  def configuration: Configuration = shared {
    this.internal
  }

  /**
   * Performs the request on a quorum of acceptors.
   *
   * @param request Request to perform.
   * @return Responses if all requests are successful.
   */
  def quorum[R](request: BeakerGrpc.BeakerBlockingStub => R): Try[Seq[R]] = shared {
    val majority = math.ceil(this.acceptors.size * this.internal.getCluster.quorum).toInt
    val clients = Random.shuffle(this.acceptors.values).take(majority).toSeq
    Try(clients.par.map(client => Try(request(BeakerGrpc.blockingStub(client))).get).seq)
  }

  /**
   * Performs the request on all learners.
   *
   * @param request Request to perform.
   * @return Responses.
   */
  def broadcast[R](request: BeakerGrpc.BeakerBlockingStub => R): Seq[Try[R]] = shared {
    this.learners.values.toSeq.par.map(client => Try(request(BeakerGrpc.blockingStub(client)))).seq
  }

  /**
   * Performs the request on a random acceptor.
   *
   * @param request Request to perform.
   * @return Response.
   */
  def random[R](request: BeakerGrpc.BeakerBlockingStub => R): Try[R] = shared {
    Try(request(BeakerGrpc.blockingStub(Random.shuffle(this.acceptors.values).head)))
  }

  /**
   * Reconfigures the router to use the specified configuration.
   *
   * @param configuration Updated configuration.
   */
  def reconfigure(configuration: Configuration): Unit = exclusive {
    this.internal = configuration
    update(configuration.getCluster.acceptors, this.acceptors)
    update(configuration.getCluster.learners, this.learners)
  }

}

object Router {

  /**
   * Constructs a router with the specified initial configuration.
   *
   * @param initial Initial configuration.
   * @return Configured router.
   */
  def apply(initial: Configuration): Router =
    new Router(initial, mutable.Map.empty, mutable.Map.empty)

  /**
   * Reconfigures the members of a group.
   *
   * @param members Updated members.
   * @param group Current connections.
   */
  private def update(members: Seq[Address], group: mutable.Map[Address, ManagedChannel]): Unit = {
    // Remove members that left the group.
    val left = group.filterKeys(!members.contains(_))
    left.values.foreach(_.shutdown())
    group --= left.keys

    // Add members that joined the group.
    val joined = members.filterNot(group.keySet.contains)
    group ++= joined.map(m => m -> ManagedChannelBuilder.forAddress(m.name, m.port).build())
  }

}