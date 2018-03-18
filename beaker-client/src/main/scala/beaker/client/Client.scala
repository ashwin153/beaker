package beaker.client

import beaker.server.protobuf._

import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.concurrent.{Future, Promise}

/**
 * A Beaker client.
 *
 * @param channel Underlying channel.
 */
class Client(channel: ManagedChannel) {

  /**
   * Returns the revisions of the keys.
   *
   * @param keys Keys to retrieve.
   * @return Revisions of keys.
   */
  def get(keys: Set[Key]): Map[Key, Revision] =
    BeakerGrpc.blockingStub(this.channel).get(Keys(keys.toSeq)).entries

  /**
   * Returns the revision of the key.
   *
   * @param key Key to retrieve.
   * @return Revision of key.
   */
  def get(key: Key): Option[Revision] = get(Set(key)).get(key)

  /**
   * Conditionally applies the changes if the dependencies remain unchanged.
   *
   * @param depends Dependencies.
   * @param changes Changes to apply.
   * @return Whether or not the changes were applied.
   */
  def cas(depends: Map[Key, Version], changes: Map[Key, Value]): Boolean = {
    val rset = depends ++ changes.keySet.map(k => k -> depends.getOrElse(k, 0L))
    val wset = changes map { case (k, v) => k -> Revision(rset(k) + 1, v) }
    BeakerGrpc.blockingStub(this.channel).propose(Transaction(rset, wset)).successful
  }

  /**
   * Closes the underlying channel.
   */
  def close(): Unit = this.channel.shutdown()

  /**
   * Asynchronously applies the function to every key limit keys at a time.
   *
   * @param f Function to apply.
   * @param limit Chunk size.
   * @return Asynchronous result.
   */
  def foreach[U](f: (Key, Revision) => U, limit: Int = 10000): Future[Unit] =
    scan(_.foreach(f.tupled), limit)

  /**
   * Conditionally applies the changes and returns their updated versions.
   *
   * @param changes Changes to apply.
   * @return Updated versions.
   */
  def put(changes: Map[Key, Value]): Option[Map[Key, Version]] = {
    val latest = get(changes.keySet).mapValues(_.version)
    val update = changes.keys map { k => k -> (latest.getOrElse(k, 0) + 1) }
    if (cas(latest, changes)) Some(update.toMap) else None
  }

  /**
   * Conditionally applies the change and returns the updated version.
   *
   * @param key Key to change.
   * @param value Change to apply.
   * @return Updated version.
   */
  def put(key: Key, value: Value): Option[Version] =
    put(Map(key -> value)).flatMap(_.get(key))

  /**
   * Attempts to consistently update the cluster.
   *
   * @param cluster Updated cluster.
   * @return Whether or not the reconfiguration was successful.
   */
  def reconfigure(cluster: Cluster): Boolean =
    BeakerGrpc.blockingStub(this.channel).reconfigure(cluster).successful

  /**
   * Asynchronously applies the function to every chunk limit keys at a time.
   *
   * @param f Function to apply.
   * @param limit Chunk size.
   * @return Asynchronous result.
   */
  def scan[U](f: Map[Key, Revision] => U, limit: Int = 10000): Future[Unit] = {
    val promise = Promise[Unit]()
    var range = Range(limit = limit)

    // Asynchronously scans the remote beaker and repairs the local beaker.
    val beaker = BeakerGrpc.stub(this.channel)
    val observer: StreamObserver[Range] = beaker.scan(new StreamObserver[Revisions] {
      override def onError(throwable: Throwable): Unit =
        promise.failure(throwable)

      override def onCompleted(): Unit =
        promise.success(())

      override def onNext(revisions: Revisions): Unit = {
        f(revisions.entries)
        range = range.copy(after = revisions.entries.keys.max)
        observer.onNext(range)
      }
    })

    // Block until the local beaker is refreshed.
    observer.onNext(range)
    promise.future
  }

  /**
   * Returns the current view of the cluster.
   *
   * @return Cluster view.
   */
  def view(): Cluster =
    BeakerGrpc.blockingStub(this.channel).view(Void())

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
    new Client(ManagedChannelBuilder.forAddress(name, port).build())

}
