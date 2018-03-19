package beaker.client

import beaker.common.util._
import beaker.server.protobuf._

import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * A Beaker client.
 *
 * @param channel Underlying channel.
 */
class Client(channel: ManagedChannel) {

  /**
   * Closes the underlying channel.
   */
  def close(): Unit = this.channel.shutdown()

  /**
   * Returns the revisions of the keys.
   *
   * @param keys Keys to retrieve.
   * @return Revisions of keys.
   */
  def get(keys: Set[Key]): Try[Map[Key, Revision]] = {
    Try(BeakerGrpc.blockingStub(this.channel).get(Keys(keys.toSeq)).entries)
  }

  /**
   * Returns the revision of the key.
   *
   * @param key Key to retrieve.
   * @return Revision of key.
   */
  def get(key: Key): Try[Option[Revision]] = {
    get(Set(key)).map(_.get(key))
  }

  /**
   * Asynchronously applies the function to every chunk limit keys at a time.
   *
   * @param f Function to apply.
   * @param limit Chunk size.
   * @return Asynchronous result.
   */
  def scan[U](f: Map[Key, Revision] => U, limit: Int = 10000): Future[Unit] = {
    val promise = Promise[Unit]()
    var range = Range(None, limit)
    val server = new AtomicReference[StreamObserver[Range]]

    // Asynchronously scans the remote beaker and repairs the local beaker.
    val observer = BeakerGrpc.stub(this.channel).scan(new StreamObserver[Revisions] {
      override def onError(throwable: Throwable): Unit =
        promise.failure(throwable)

      override def onCompleted(): Unit =
        promise.success(())

      override def onNext(revisions: Revisions): Unit = {
        f(revisions.entries)
        range = range.withAfter(revisions.entries.keys.max)
        server.get().onNext(range)
      }
    })

    // Block until the local beaker is refreshed.
    server.set(observer)
    observer.onNext(range)
    promise.future
  }

  /**
   * Asynchronously applies the function to every key limit keys at a time.
   *
   * @param f Function to apply.
   * @param limit Chunk size.
   * @return Asynchronous result.
   */
  def foreach[U](f: (Key, Revision) => U, limit: Int = 10000): Future[Unit] = {
    scan(_.foreach(f.tupled), limit)
  }

  /**
   * Conditionally applies the changes if the dependencies remain unchanged.
   *
   * @param depends Dependencies.
   * @param changes Changes to apply.
   * @return Whether or not the changes were applied.
   */
  def cas(depends: Map[Key, Version], changes: Map[Key, Value]): Try[Map[Key, Version]] = {
    // Implicitly depend on the initial version of every key that is changed but not depended on.
    val rset = depends ++ changes.keySet.map(k => k -> depends.getOrElse(k, 0L))
    val wset = changes map { case (k, v) => k -> Revision(rset(k) + 1, v) }

    // Propose a transaction and return the updated versions if successful.
    Try(BeakerGrpc.blockingStub(this.channel).propose(Transaction(rset, wset)))
      .filter(_.successful)
      .map(_ => wset.mapValues(_.version))
  }

  /**
   * Conditionally applies the changes and returns their updated versions.
   *
   * @param changes Changes to apply.
   * @return Updated versions.
   */
  def put(changes: Map[Key, Value]): Try[Map[Key, Version]] = {
    get(changes.keySet).map(_.mapValues(_.version)).flatMap(cas(_, changes))
  }

  /**
   * Conditionally applies the change and returns the updated version.
   *
   * @param key Key to change.
   * @param value Change to apply.
   * @return Updated version.
   */
  def put(key: Key, value: Value): Try[Version] = {
    put(Map(key -> value)).flatMap(_.get(key).toTry)
  }

  /**
   * Attempts to consistently update the network configuration.
   *
   * @param configuration Updated configuration.
   * @return Whether or not the reconfiguration was successful.
   */
  def reconfigure(configuration: Configuration): Boolean = {
    BeakerGrpc.blockingStub(this.channel).reconfigure(configuration).successful
  }

  /**
   * Returns the current view of the network configuration.
   *
   * @return Network configuration.
   */
  def network(): Configuration = {
    BeakerGrpc.blockingStub(this.channel).network(Void())
  }

  /**
   * Makes a promise not to accept any proposal that conflicts with the proposal it returns and has
   * a lower ballot than the proposal it receives. If a promise has been made to a newer proposal,
   * its ballot is returned. If older proposals have already been accepted, they are merged together
   * and returned. Otherwise, it returns the proposal it receives with the default ballot.
   *
   * @param proposal Proposal to prepare.
   * @return Promise.
   */
  def prepare(proposal: Proposal): Try[Proposal] = {
    Try(BeakerGrpc.blockingStub(this.channel).prepare(proposal))
  }

  /**
   * Requests a vote for a proposal. A vote is cast for a proposal if and only if a promise has
   * not been made to a newer proposal.
   *
   * @param proposal Proposal to vote for.
   * @return Whether or not a vote was cast.
   */
  def accept(proposal: Proposal): Try[Unit] = {
    Try(BeakerGrpc.blockingStub(this.channel).accept(proposal)).filter(_.successful).map(_ => ())
  }

  /**
   * Casts a vote for a proposal. A proposal is committed once a majority of acceptors in its
   * network configuration have voted for it.
   *
   * @param proposal Proposal to commit.
   * @return Whether or not a vote was successfully cast.
   */
  def learn(proposal: Proposal): Try[Unit] = {
    Try(BeakerGrpc.blockingStub(this.channel).learn(proposal)).map(_ => ())
  }

}

object Client {

  /**
   * Constructs a client connected to the specified address.
   *
   * @param address Network location.
   * @return Connected client.
   */
  def apply(address: Address): Client =
    Client(address.name, address.port)

  /**
   * Constructs a client connected to the specified host.
   *
   * @param name Hostname.
   * @param port Port number.
   * @return Connected client.
   */
  def apply(name: String, port: Int): Client =
    new Client(ManagedChannelBuilder.forAddress(name, port).usePlaintext(true).build())

}
