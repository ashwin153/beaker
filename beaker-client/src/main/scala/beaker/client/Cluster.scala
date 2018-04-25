package beaker.client

import beaker.common.concurrent.Locking
import beaker.server.protobuf.Address

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._
import scala.util._

/**
 * A thread-safe collection of clients.
 *
 * @param members Cluster member.
 */
class Cluster(members: mutable.Map[Address, Client]) extends Locking {

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
  def broadcastAsync[T](request: Client => Future[T]): Future[Seq[T]] = shared {
    Future.sequence(this.members.values.toSeq.map(request))
  }

  /**
   * Closes all members of the cluster.
   */
  def close(): Unit = {
    this.members.keys.foreach(leave)
  }

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
   * Performs the request on a quorum of members in the cluster and returns successfully if and only
   * if all requests complete successfully.
   *
   * @param request Request to perform.
   * @return Sequence of responses.
   */
  def quorum[T](request: Client => Try[T]): Try[Seq[T]] = shared {
    val clients = Random.shuffle(this.members.values.toSeq).take(this.members.size / 2 + 1)
    val responses = clients.par.map(client => request(client)).filter(_.isSuccess).map(_.get).seq
    Try(responses).filter(_.size >= this.members.size / 2 + 1)
  }

  /**
   * Asynchronously performs the request on a quorum of members in the cluster and returns
   * successfully if and only if all requests complete successfully.
   *
   * @param request Request to perform.
   * @return Sequence of responses.
   */
  def quorumAsync[T](request: Client => Future[T]): Future[Seq[T]] = shared {
    val clients = Random.shuffle(this.members.values.toSeq).take(this.members.size / 2 + 1)
    val requests = clients.map(c => request(c) map { Success(_) } recover { case x => Failure(x) })
    val responses = Future.sequence(requests).map(_.filter(_.isSuccess).map(_.get))
    responses.filter(_.size >= this.members.size / 2 + 1)
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
   * Returns the number of members in the cluster.
   *
   * @return Number of members.
   */
  def size: Int = shared {
    this.members.size
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
  def apply(members: Seq[Address]): Cluster = {
    val cluster = Cluster.empty
    members.foreach(cluster.join)
    cluster
  }

}