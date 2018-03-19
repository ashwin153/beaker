package beaker.client

import beaker.common.concurrent.Locking
import beaker.server.protobuf.Address

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Random, Try}

/**
 * A thread-safe collection of clients.
 *
 * @param members Cluster member.
 */
class Cluster(members: mutable.Map[Address, Client]) extends Locking {

  /**
   * Adds the member to the cluster.
   *
   * @param member Member to add.
   */
  def join(member: Address): Unit = exclusive {
    this.members.getOrElseUpdate(member, Client(member))
  }

  /**
   * Removes the member from the cluster.
   *
   * @param member Member to remove.
   */
  def leave(member: Address): Unit = exclusive {
    this.members.remove(member).foreach(_.close())
  }

  /**
   * Updates the members of the cluster.
   *
   * @param members Updated members.
   */
  def update(members: Seq[Address]): Unit = {
    this.members.keys.filterNot(members.contains).foreach(leave)
    members.filterNot(this.members.contains).foreach(join)
  }

  /**
   * Performs the request on all members of the cluster.
   *
   * @param request Request to perform.
   * @return Sequence of responses.
   */
  def broadcast[T](request: Client => Try[T]): Seq[Try[T]] = shared {
    this.members.values.toSeq.par.map(client => request(client)).seq
  }

  /**
   * Asynchronously performs the request on all members of the cluster.
   *
   * @param request Request to perform.
   * @return Sequence of responses.
   */
  def broadcastAsync[T](request: Client => Future[T]): Seq[Future[T]] = shared {
    this.members.values.toSeq.map(request)
  }

  /**
   * Performs the request on a random member of the cluster.
   *
   * @param request Request to perform.
   * @return Response.
   */
  def random[T](request: Client => Try[T]): Try[T] = shared {
    request(Random.shuffle(this.members.values).head)
  }

  /**
   * Asynchronously performs the request on a random member of the cluster.
   *
   * @param request Request to perform.
   * @return Response.
   */
  def randomAsync[T](request: Client => Future[T]): Future[T] = shared {
    request(Random.shuffle(this.members.values).head)
  }

  /**
   * Performs the request on a quorum of members in the cluster and returns successfully if and only
   * if all requests complete successfully.
   *
   * @param request Request to perform.
   * @param of Quorum size. (0.50, 1]
   * @return Sequence of responses.
   */
  def quorum[T](request: Client => Try[T], of: Double = 0.51): Try[Seq[T]] = shared {
    require(of > 0.5 && of <= 1, s"Invalid quorum size $of.")
    val majority = math.ceil(this.members.size * of).toInt + 1
    val clients = Random.shuffle(this.members.values.toSeq).take(majority)
    Try(clients.par.map(client => request(client).get).seq)
  }

  /**
   * Asynchronously performs the request on a quorum of members in the cluster and returns
   * successfully if and only if all requests complete successfully.
   *
   * @param request Request to perform.
   * @param of Quorum size. (0.5, 1]
   * @return Sequence of responses.
   */
  def quorumAsync[T](request: Client => Future[T], of: Double = 0.51): Future[Seq[T]] = shared {
    require(of > 0.5 && of <= 1, s"Invalid quorum size $of.")
    val majority = math.ceil(this.members.size * of).toInt + 1
    val clients = Random.shuffle(this.members.values.toSeq).take(majority)
    Future.sequence(clients.map(request))
  }

}

object Cluster {

  /**
   * Constructs an empty cluster.
   *
   * @return Empty cluster.
   */
  def empty: Cluster = new Cluster(mutable.Map.empty)

  /**
   * Constructs a cluster initialized with the specified members.
   *
   * @param members Initial members.
   * @return Initialized cluster.
   */
  def apply(members: Seq[Address]): Cluster = Cluster(members: _*)

  /**
   * Constructs a cluster initialized with the specified members.
   *
   * @param members Initial members.
   * @return Initialized cluster.
   */
  def apply(members: Address*): Cluster = {
    val cluster = Cluster.empty
    members.foreach(cluster.join)
    cluster
  }

}